use crossbeam_skiplist::map::Entry;
use bytes::Bytes;
pub struct MemTableIterator {
    current: Option<Entry<Bytes, super::memtable::EntryValue>>,
}
