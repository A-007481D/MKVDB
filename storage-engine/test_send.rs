use crossbeam_skiplist::SkipMap;
fn assert_send<T: Send>() {}
fn main() {
    assert_send::<crossbeam_skiplist::map::Iter<'static, i32, i32>>();
}
