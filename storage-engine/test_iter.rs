use crossbeam_skiplist::SkipMap;
use bytes::Bytes;
fn test(map: &SkipMap<Bytes, Bytes>) {
    let _ = map.range(..);
}
