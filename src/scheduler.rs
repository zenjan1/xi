//! 内核进程调度器 — 优先级任务队列
use std::cmp::Ordering;
use std::sync::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum TaskPriority { Low = 0, Normal = 1, High = 2, Critical = 3 }

#[derive(Debug, Clone)]
pub struct QueueItem { pub priority: TaskPriority, pub task_id: String, pub seq: u64 }

impl QueueItem {
    fn cmp(&self, other: &Self) -> Ordering {
        other.priority.cmp(&self.priority).then(self.seq.cmp(&other.seq))
    }
}

pub struct TaskQueue {
    items: Mutex<Vec<QueueItem>>,
    seq: Mutex<u64>,
}

impl TaskQueue {
    pub fn new() -> Self { Self { items: Mutex::new(Vec::new()), seq: Mutex::new(0) } }
    pub fn enqueue(&self, priority: TaskPriority, task_id: impl Into<String>) {
        let seq = { let mut s = self.seq.lock().unwrap(); *s += 1; *s };
        self.items.lock().unwrap().push(QueueItem { priority, task_id: task_id.into(), seq });
    }
    pub fn dequeue(&self) -> Option<(TaskPriority, String)> {
        let mut items = self.items.lock().unwrap();
        if items.is_empty() { return None; }
        let mut best = 0;
        for i in 1..items.len() { if items[i].cmp(&items[best]) == Ordering::Greater { best = i; } }
        let item = items.remove(best);
        Some((item.priority, item.task_id))
    }
    pub fn len(&self) -> usize { self.items.lock().unwrap().len() }
    pub fn is_empty(&self) -> bool { self.len() == 0 }
}

impl Default for TaskQueue { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_priority_order() {
        let q = TaskQueue::new();
        q.enqueue(TaskPriority::Low, "low");
        q.enqueue(TaskPriority::Critical, "critical");
        q.enqueue(TaskPriority::Normal, "normal");
        q.enqueue(TaskPriority::High, "high");
        assert_eq!(q.dequeue().unwrap(), (TaskPriority::Critical, "critical".into()));
        assert_eq!(q.dequeue().unwrap(), (TaskPriority::High, "high".into()));
        assert_eq!(q.dequeue().unwrap(), (TaskPriority::Normal, "normal".into()));
        assert_eq!(q.dequeue().unwrap(), (TaskPriority::Low, "low".into()));
        assert!(q.dequeue().is_none());
    }

    #[test]
    fn test_fifo_same_priority() {
        let q = TaskQueue::new();
        q.enqueue(TaskPriority::Normal, "first");
        q.enqueue(TaskPriority::Normal, "second");
        assert_eq!(q.dequeue().unwrap().1, "first");
        assert_eq!(q.dequeue().unwrap().1, "second");
    }

    #[test]
    fn test_concurrent() {
        let q = Arc::new(TaskQueue::new());
        let mut handles = vec![];
        for t in 0..10 {
            let q = q.clone();
            handles.push(thread::spawn(move || { for i in 0..100 { q.enqueue(TaskPriority::Normal, format!("t{}-{}", t, i)); } }));
        }
        for h in handles { h.join().unwrap(); }
        assert_eq!(q.len(), 1000);
        for _ in 0..1000 { assert!(q.dequeue().is_some()); }
        assert!(q.is_empty());
    }
}
