use bytes::Bytes;
use proptest::prelude::*;
use std::collections::HashMap;
use storage_engine::engine::ApexEngine;
use tempfile::tempdir;

#[test]
fn test_basic_put_get_delete() {
    let dir = tempdir().unwrap();
    let engine = ApexEngine::open(dir.path()).unwrap();

    let key = Bytes::from("hello");
    let val = Bytes::from("world");

    // Put
    engine.put(key.clone(), val.clone()).unwrap();

    // Get
    let result = engine.get(&key).unwrap();
    assert_eq!(result, Some(val));

    // Delete
    engine.delete(key.clone()).unwrap();

    // Get again — tombstone must suppress the value
    let result2 = engine.get(&key).unwrap();
    assert_eq!(result2, None);
}

#[test]
fn test_crash_recovery_via_wal_replay() {
    let dir = tempdir().unwrap();

    let key1 = Bytes::from("k1");
    let val1 = Bytes::from("v1");
    let key2 = Bytes::from("k2");
    let val2 = Bytes::from("v2");

    {
        let engine = ApexEngine::open(dir.path()).unwrap();
        engine.put(key1.clone(), val1.clone()).unwrap();
        engine.put(key2.clone(), val2.clone()).unwrap();
        engine.delete(key1.clone()).unwrap();
        // Engine is dropped — data lives only in the WAL, not flushed to SSTable.
    }

    {
        // Reopen — WAL replay must reconstruct the exact state.
        let engine = ApexEngine::open(dir.path()).unwrap();
        let result1 = engine.get(&key1).unwrap();
        let result2 = engine.get(&key2).unwrap();
        assert_eq!(result1, None, "k1 was deleted; should not reappear after recovery");
        assert_eq!(result2, Some(val2), "k2 must survive recovery");
    }
}

#[test]
fn test_overwrite_survives_recovery() {
    let dir = tempdir().unwrap();
    let key = Bytes::from("counter");

    {
        let engine = ApexEngine::open(dir.path()).unwrap();
        engine.put(key.clone(), Bytes::from("1")).unwrap();
        engine.put(key.clone(), Bytes::from("2")).unwrap();
        engine.put(key.clone(), Bytes::from("3")).unwrap();
    }

    {
        let engine = ApexEngine::open(dir.path()).unwrap();
        let val = engine.get(&key).unwrap();
        assert_eq!(val, Some(Bytes::from("3")), "latest overwrite must win after recovery");
    }
}

// ---------------------------------------------------------------------------
// Property-based testing
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
#[allow(dead_code)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Delete(Vec<u8>),
}

#[allow(dead_code)]
fn op_strategy() -> impl Strategy<Value = Op> {
    prop_oneof![
        (any::<Vec<u8>>(), any::<Vec<u8>>()).prop_map(|(k, v)| Op::Put(k, v)),
        any::<Vec<u8>>().prop_map(Op::Delete),
    ]
}

proptest! {
    // Uncomment the #[test] attribute to run the full fuzzer:
    // #[test]
    fn test_engine_state_machine(ops in prop::collection::vec(op_strategy(), 1..100)) {
        let dir = tempdir().unwrap();
        let engine = ApexEngine::open(dir.path()).unwrap();
        let mut model: HashMap<Vec<u8>, Bytes> = HashMap::new();

        for op in ops {
            match op {
                Op::Put(k, v) => {
                    let key = Bytes::from(k.clone());
                    let val = Bytes::from(v.clone());
                    engine.put(key, val.clone()).unwrap();
                    model.insert(k, val);
                }
                Op::Delete(k) => {
                    let key = Bytes::from(k.clone());
                    engine.delete(key).unwrap();
                    model.remove(&k);
                }
            }
        }

        for (k, expected_v) in &model {
            let actual_v = engine.get(k).unwrap();
            assert_eq!(actual_v.as_ref(), Some(expected_v));
        }
    }
}
