//! 内核事件总线 — 模块间 pub/sub 解耦通信
use std::collections::VecDeque;
use std::sync::{Arc, Mutex, RwLock};

#[derive(Debug, Clone)]
pub enum Event {
    TaskStarted { task_id: u32, title: String },
    TaskCompleted { task_id: u32 },
    TaskFailed { task_id: u32, msg: String },
    ModuleLoaded { name: String },
    ModuleUnloaded { name: String },
    Heartbeat { ts: u64 },
}

impl Event {
    pub fn label(&self) -> String {
        match self {
            Event::TaskStarted { task_id, title } => format!("task#{} started: {}", task_id, title),
            Event::TaskCompleted { task_id } => format!("task#{} completed", task_id),
            Event::TaskFailed { task_id, msg } => format!("task#{} failed: {}", task_id, msg),
            Event::ModuleLoaded { name } => format!("module loaded: {}", name),
            Event::ModuleUnloaded { name } => format!("module unloaded: {}", name),
            Event::Heartbeat { ts } => format!("heartbeat@{}", ts),
        }
    }
}

pub type EventHandler = Arc<dyn Fn(&Event) + Send + Sync>;

pub trait EventBus: Send + Sync {
    fn subscribe(&self, handler: EventHandler);
    fn publish(&self, event: &Event);
    fn handler_count(&self) -> usize;
    fn recent(&self, n: usize) -> Vec<Event>;
}

pub struct SimpleEventBus {
    handlers: RwLock<Vec<EventHandler>>,
    log: Mutex<VecDeque<Event>>,
    capacity: usize,
}

impl SimpleEventBus {
    pub fn new() -> Self {
        Self { handlers: RwLock::new(Vec::new()), log: Mutex::new(VecDeque::with_capacity(1024)), capacity: 1024 }
    }
    pub fn with_capacity(capacity: usize) -> Self {
        Self { handlers: RwLock::new(Vec::new()), log: Mutex::new(VecDeque::with_capacity(capacity)), capacity }
    }
}

impl Default for SimpleEventBus { fn default() -> Self { Self::new() } }

impl EventBus for SimpleEventBus {
    fn subscribe(&self, handler: EventHandler) { self.handlers.write().unwrap().push(handler); }
    fn publish(&self, event: &Event) {
        { let mut log = self.log.lock().unwrap(); log.push_back(event.clone()); while log.len() > self.capacity { log.pop_front(); } }
        for h in self.handlers.read().unwrap().iter() { h(event); }
    }
    fn handler_count(&self) -> usize { self.handlers.read().unwrap().len() }
    fn recent(&self, n: usize) -> Vec<Event> {
        let log = self.log.lock().unwrap(); let start = log.len().saturating_sub(n); log.iter().skip(start).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn test_subscribe_publish() {
        let bus = SimpleEventBus::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        bus.subscribe(Arc::new(move |_| c.fetch_add(1, Ordering::SeqCst)));
        bus.publish(&Event::Heartbeat { ts: 1 });
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multi_subscriber() {
        let bus = SimpleEventBus::new();
        let (c1, c2, c3) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (a, b, c) = (c1.clone(), c2.clone(), c3.clone());
        bus.subscribe(Arc::new(move |_| a.fetch_add(1, Ordering::SeqCst)));
        bus.subscribe(Arc::new(move |_| b.fetch_add(1, Ordering::SeqCst)));
        bus.subscribe(Arc::new(move |_| c.fetch_add(1, Ordering::SeqCst)));
        bus.publish(&Event::TaskStarted { task_id: 1, title: "t".into() });
        assert_eq!(c1.load(Ordering::SeqCst), 1);
        assert_eq!(c2.load(Ordering::SeqCst), 1);
        assert_eq!(c3.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_ring_buffer_overflow() {
        let bus = SimpleEventBus::with_capacity(100);
        for i in 0..200 { bus.publish(&Event::Heartbeat { ts: i }); }
        let recent = bus.recent(100);
        assert_eq!(recent.len(), 100);
        match &recent[99] { Event::Heartbeat { ts } => assert_eq!(*ts, 199), _ => panic!() }
    }

    #[test]
    fn test_event_label() {
        assert_eq!(Event::TaskFailed { task_id: 42, msg: "timeout".into() }.label(), "task#42 failed: timeout");
    }
}
