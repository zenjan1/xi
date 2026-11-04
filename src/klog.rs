//! 内核日志 — ring buffer + 级别过滤
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel { Trace = 0, Debug = 1, Info = 2, Warn = 3, Error = 4 }

impl LogLevel {
    pub fn as_str(&self) -> &'static str {
        match self { LogLevel::Trace => "TRACE", LogLevel::Debug => "DEBUG", LogLevel::Info => "INFO", LogLevel::Warn => "WARN", LogLevel::Error => "ERROR" }
    }
}

#[derive(Debug, Clone)]
pub struct LogEntry { pub level: LogLevel, pub msg: String, pub ts_ms: u64 }

pub struct KernelLogger {
    buffer: Mutex<VecDeque<LogEntry>>,
    min_level: AtomicU8,
    capacity: usize,
}

impl KernelLogger {
    pub fn new() -> Self { Self { buffer: Mutex::new(VecDeque::with_capacity(4096)), min_level: AtomicU8::new(2), capacity: 4096 } }
    pub fn with_capacity(capacity: usize) -> Self { Self { buffer: Mutex::new(VecDeque::with_capacity(capacity)), min_level: AtomicU8::new(2), capacity } }
    pub fn set_min_level(&self, level: LogLevel) { self.min_level.store(level as u8, Ordering::SeqCst); }
    pub fn log(&self, level: LogLevel, msg: &str) {
        if (level as u8) < self.min_level.load(Ordering::SeqCst) { return; }
        let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64;
        let mut buf = self.buffer.lock().unwrap();
        buf.push_back(LogEntry { level, msg: msg.to_string(), ts_ms });
        while buf.len() > self.capacity { buf.pop_front(); }
    }
    pub fn recent(&self, n: usize) -> Vec<LogEntry> {
        let buf = self.buffer.lock().unwrap(); let start = buf.len().saturating_sub(n); buf.iter().skip(start).cloned().collect()
    }
    pub fn len(&self) -> usize { self.buffer.lock().unwrap().len() }
}

impl Default for KernelLogger { fn default() -> Self { Self::new() } }

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_level_filter() {
        let logger = KernelLogger::new();
        logger.set_min_level(LogLevel::Warn);
        logger.log(LogLevel::Info, "skip");
        logger.log(LogLevel::Warn, "warning");
        logger.log(LogLevel::Error, "error");
        assert_eq!(logger.len(), 2);
        let r = logger.recent(10);
        assert_eq!(r[0].level, LogLevel::Warn);
        assert_eq!(r[1].level, LogLevel::Error);
    }

    #[test]
    fn test_overflow() {
        let logger = KernelLogger::with_capacity(100);
        for i in 0..500 { logger.log(LogLevel::Info, &format!("msg {}", i)); }
        assert_eq!(logger.len(), 100);
        assert!(logger.recent(100)[99].msg.contains("499"));
    }

    #[test]
    fn test_timestamp_increasing() {
        let logger = KernelLogger::new();
        for _ in 0..100 { logger.log(LogLevel::Info, "tick"); }
        let r = logger.recent(100);
        for i in 1..r.len() { assert!(r[i].ts_ms >= r[i - 1].ts_ms); }
    }

    #[test]
    fn test_default_min_level() {
        let logger = KernelLogger::new();
        logger.log(LogLevel::Debug, "skip");
        assert_eq!(logger.len(), 0);
        logger.log(LogLevel::Info, "ok");
        assert_eq!(logger.len(), 1);
    }
}
