use rand::{Rng, SeedableRng, rngs::StdRng};
use std::sync::{Arc, RwLock};
use std::time::Duration;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub enum FaultRule {
    Partition(u64, u64),
    Delay(u64, u64, Duration),
    RandomDelay(u64, u64, Duration, Duration), // (src, dst, min, max)
    Drop(u64, u64, f64),                       // (src, dst, probability)
    Duplicate(u64, u64, f64),                  // (src, dst, probability)
}

pub struct FaultController {
    seed: u64,
    rng: Mutex<StdRng>,
    rules: RwLock<Vec<FaultRule>>,
}

use std::sync::Mutex;

impl FaultController {
    pub fn new(seed: u64) -> Arc<Self> {
        Arc::new(Self {
            seed,
            rng: Mutex::new(StdRng::seed_from_u64(seed)),
            rules: RwLock::new(Vec::new()),
        })
    }

    pub fn add_rule(&self, rule: FaultRule) {
        let mut rules = self.rules.write().unwrap();
        rules.push(rule);
    }

    pub fn clear_rules(&self) {
        let mut rules = self.rules.write().unwrap();
        rules.clear();
    }

    pub fn get_action(&self, src: u64, dst: u64) -> AppliedAction {
        let rules = self.rules.read().unwrap();
        let mut action = AppliedAction::default();

        for rule in rules.iter() {
            match rule {
                FaultRule::Partition(a, b) => {
                    if (src == *a && dst == *b) || (src == *b && dst == *a) {
                        action.drop = true;
                        // Using println! here because we want to see it in --nocapture without delay
                        // println!("[DEBUG] FaultController: Partition match for {} <-> {}", src, dst);
                    }
                }
                FaultRule::Delay(a, b, d) => {
                    if src == *a && dst == *b {
                        action.delay = Some(*d);
                    }
                }
                FaultRule::RandomDelay(a, b, min, max) => {
                    if src == *a && dst == *b {
                        let mut rng = self.rng.lock().unwrap();
                        let millis = rng.random_range(min.as_millis()..max.as_millis());
                        action.delay = Some(Duration::from_millis(millis as u64));
                    }
                }
                FaultRule::Drop(a, b, p) => {
                    if src == *a && dst == *b {
                        let mut rng = self.rng.lock().unwrap();
                        if rng.random_bool(*p) {
                            action.drop = true;
                        }
                    }
                }
                FaultRule::Duplicate(a, b, p) => {
                    if src == *a && dst == *b {
                        let mut rng = self.rng.lock().unwrap();
                        if rng.random_bool(*p) {
                            action.duplicate = true;
                        }
                    }
                }
            }
        }
        action
    }

    #[allow(dead_code)]
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

#[derive(Default)]
pub struct AppliedAction {
    pub drop: bool,
    pub delay: Option<Duration>,
    pub duplicate: bool,
}
