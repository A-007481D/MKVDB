use crossbeam_skiplist::SkipMap;
use bytes::Bytes;
fn test(map: &SkipMap<Bytes, Bytes>, key: &Bytes) {
    let entry = map.lower_bound(std::ops::Bound::Excluded(key));
}
