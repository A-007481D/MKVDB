use bytes::Bytes;

/// A collection of write operations that are applied atomically to the database.
///
/// WriteBatches ensure that multiple updates succeed or fail together, providing
/// a foundational building block for transactions and consistent replication.
#[derive(Default, Clone)]
pub struct WriteBatch {
    pub(crate) ops: Vec<BatchOp>,
}

#[derive(Clone)]
pub(crate) enum BatchOp {
    Put(Bytes, Bytes),
    Delete(Bytes),
}

impl WriteBatch {
    /// Creates a new, empty write batch.
    #[must_use]
    pub fn new() -> Self {
        Self { ops: Vec::new() }
    }

    /// Adds a PUT operation to the batch.
    pub fn put(&mut self, key: Bytes, value: Bytes) {
        self.ops.push(BatchOp::Put(key, value));
    }

    /// Adds a DELETE operation to the batch.
    pub fn delete(&mut self, key: Bytes) {
        self.ops.push(BatchOp::Delete(key));
    }

    /// Returns the number of operations in the batch.
    #[must_use]
    pub fn len(&self) -> usize {
        self.ops.len()
    }

    /// Returns true if the batch is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }
}
