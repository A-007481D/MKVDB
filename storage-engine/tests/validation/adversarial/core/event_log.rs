use std::sync::{Arc, Mutex};
use std::time::Instant;
use openraft::ServerState;

#[derive(Debug, Clone)]
pub enum AdversarialEvent {
    RpcSent { from: u64, to: u64, msg: String },
    RpcReceived { from: u64, to: u64, msg: String, success: bool },
    FaultInjected { from: u64, to: u64, rule: String },
    NodeStateChanged { id: u64, state: ServerState },
    TestInfo(String),
}

pub struct EventLog {
    start_time: Instant,
    events: Mutex<Vec<(f64, AdversarialEvent)>>,
}

impl EventLog {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            start_time: Instant::now(),
            events: Mutex::new(Vec::new()),
        })
    }

    pub fn log(&self, event: AdversarialEvent) {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        let mut events = self.events.lock().unwrap();
        
        // Print to stderr for immediate feedback during tests
        eprintln!("[{:.3}s] {:?}", elapsed, event);
        
        events.push((elapsed, event));
    }

    pub fn dump(&self) {
        let events = self.events.lock().unwrap();
        eprintln!("\n--- ADVERSARIAL EVENT LOG DUMP ---");
        for (time, event) in events.iter() {
            eprintln!("[{:.3}s] {:?}", time, event);
        }
        eprintln!("--- END DUMP ---\n");
    }

    pub fn clear(&self) {
        let mut events = self.events.lock().unwrap();
        events.clear();
    }
}
