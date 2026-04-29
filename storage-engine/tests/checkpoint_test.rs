#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use std::fs;
    use storage_engine::ApexEngine;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_checkpoint_consistency() {
        let dir = tempdir().unwrap();
        let engine = ApexEngine::open(dir.path()).unwrap();

        // 1. Write some data
        engine
            .put(Bytes::from("key1"), Bytes::from("val1"))
            .await
            .unwrap();
        engine
            .put(Bytes::from("key2"), Bytes::from("val2"))
            .await
            .unwrap();

        // Force a flush to create an SSTable
        engine.force_flush().unwrap();
        // Wait a bit for background flush to finish
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        // Write more data to the WAL
        engine
            .put(Bytes::from("key3"), Bytes::from("val3"))
            .await
            .unwrap();

        // 2. Create checkpoint (Async)
        let cp_dir = tempdir().unwrap();
        let cp_path = cp_dir.path().join("cp1");
        engine
            .create_checkpoint(&cp_path)
            .await
            .expect("Failed to create checkpoint");

        // 3. Write data AFTER checkpoint start
        engine
            .put(Bytes::from("key_after"), Bytes::from("val_after"))
            .await
            .unwrap();

        // 4. Verify checkpoint
        assert!(cp_path.join("MANIFEST").exists());

        // Ensure at least one SST and one WAL exists
        let has_sst = fs::read_dir(&cp_path)
            .unwrap()
            .any(|e| e.unwrap().file_name().to_str().unwrap().ends_with(".sst"));
        let has_wal = fs::read_dir(&cp_path)
            .unwrap()
            .any(|e| e.unwrap().file_name().to_str().unwrap().ends_with(".wal"));
        assert!(has_sst, "Checkpoint should contain at least one SSTable");
        assert!(has_wal, "Checkpoint should contain at least one WAL");

        // 5. Open checkpoint as a new engine
        let cp_engine = ApexEngine::open(&cp_path).expect("Failed to open checkpointed engine");
        assert_eq!(cp_engine.get(b"key1").unwrap(), Some(Bytes::from("val1")));
        assert_eq!(cp_engine.get(b"key2").unwrap(), Some(Bytes::from("val2")));
        assert_eq!(cp_engine.get(b"key3").unwrap(), Some(Bytes::from("val3")));

        // CRITICAL: key_after should NOT be in the checkpoint
        assert_eq!(
            cp_engine.get(b"key_after").unwrap(),
            None,
            "Checkpoint contained data written after snapshot start"
        );

        // Ensure writing to the original engine doesn't affect the checkpoint
        engine
            .put(Bytes::from("key4"), Bytes::from("val4"))
            .await
            .unwrap();
        assert_eq!(cp_engine.get(b"key4").unwrap(), None);
    }
}
