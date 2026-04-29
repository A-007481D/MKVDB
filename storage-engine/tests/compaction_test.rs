use storage_engine::{ApexEngine, SyncPolicy};
use futures_util::StreamExt;
use bytes::Bytes;
use tempfile::tempdir;
use std::time::Duration;

#[tokio::test]
async fn test_compaction_shrink() {
    let dir = tempdir().unwrap();
    let engine = ApexEngine::open_with_policy(dir.path(), SyncPolicy::EveryWrite).unwrap();

    // 1. Insert data and force flushes to create multiple L0 files
    for i in 0..5 {
        let key = Bytes::from(format!("key_{:03}", i));
        let val = Bytes::from(format!("value_{:03}", i));
        engine.put(key, val).await.unwrap();
        engine.force_flush().unwrap();
    }

    // 2. Wait for background compaction to trigger (needs 4+ files)
    // We'll poll the directory to see if file count decreases
    let mut compacted = false;
    for _ in 0..20 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let sst_count = std::fs::read_dir(dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "sst"))
            .count();
        
        // We started with 5 files. After compaction, they should be merged into L1.
        // Depending on TARGET_SST_SIZE, it might be 1 or more, but definitely < 5.
        if sst_count < 5 && sst_count > 0 {
            compacted = true;
            break;
        }
    }

    assert!(compacted, "Compaction did not decrease SST file count");

    // 3. Verify data is still there
    for i in 0..5 {
        let key = format!("key_{:03}", i);
        let expected = format!("value_{:03}", i);
        let val = engine.get(key.as_bytes()).unwrap().unwrap();
        assert_eq!(val.as_ref(), expected.as_bytes());
    }
}

#[tokio::test]
async fn test_ghost_read_concurrency() {
    let dir = tempdir().unwrap();
    let engine = std::sync::Arc::new(ApexEngine::open_with_policy(dir.path(), SyncPolicy::EveryWrite).unwrap());

    // Populate some initial data
    for i in 0..100 {
        let key = Bytes::from(format!("key_{:03}", i));
        let val = Bytes::from(format!("value_{:03}", i));
        engine.put(key, val).await.unwrap();
    }
    engine.force_flush().unwrap();

    let engine_cloned = engine.clone();
    
    // Spawn a reader that performs a long scan
    let reader_handle = tokio::spawn(async move {
        let mut scan = engine_cloned.scan(Bytes::from("key_000"), Bytes::from("key_099")).unwrap();
        let mut count = 0;
        while let Some(res) = scan.next().await {
            let _ = res.unwrap();
            count += 1;
            // Artificial delay to simulate long-running scan
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        count
    });

    // While reader is scanning, trigger multiple compactions/flushes to change the version
    for i in 100..200 {
        let key = Bytes::from(format!("key_{:03}", i));
        let val = Bytes::from(format!("value_{:03}", i));
        engine.put(key, val).await.unwrap();
        if i % 10 == 0 {
            engine.force_flush().unwrap();
        }
    }

    let count = reader_handle.await.unwrap();
    // The reader should see exactly 100 keys from its initial snapshot,
    // even though the underlying files changed.
    assert_eq!(count, 100);
}

#[tokio::test]
async fn test_tombstone_purge() {
    let dir = tempdir().unwrap();
    let engine = ApexEngine::open_with_policy(dir.path(), SyncPolicy::EveryWrite).unwrap();

    // 1. Insert a key
    engine.put(Bytes::from("key_to_delete"), Bytes::from("value")).await.unwrap();
    engine.force_flush().unwrap();

    // 2. Delete the key
    engine.delete(Bytes::from("key_to_delete")).await.unwrap();
    engine.force_flush().unwrap();

    // 3. Trigger more flushes to ensure compaction triggers
    for i in 0..10 {
        engine.put(Bytes::from(format!("other_{}", i)), Bytes::from("val")).await.unwrap();
        engine.force_flush().unwrap();
    }

    // 4. Wait for compaction
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 5. Verify the key is gone
    assert!(engine.get(b"key_to_delete").unwrap().is_none());

    // 6. Scan should not show it either
    let mut scan = engine.scan(Bytes::from("key_"), Bytes::from("key_z")).unwrap();
    assert!(scan.next().await.is_none());
}
