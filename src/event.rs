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
        bus.subscribe(Arc::new(move |_| { c.fetch_add(1, Ordering::SeqCst); }));
        bus.publish(&Event::Heartbeat { ts: 1 });
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_multi_subscriber() {
        let bus = SimpleEventBus::new();
        let (c1, c2, c3) = (Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)), Arc::new(AtomicUsize::new(0)));
        let (a, b, c) = (c1.clone(), c2.clone(), c3.clone());
        bus.subscribe(Arc::new(move |_| { a.fetch_add(1, Ordering::SeqCst); }));
        bus.subscribe(Arc::new(move |_| { b.fetch_add(1, Ordering::SeqCst); }));
        bus.subscribe(Arc::new(move |_| { c.fetch_add(1, Ordering::SeqCst); }));
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

// ===========================================================================
// 事件总线内核(Bus / Handler)
//
// 参考 Linux netlink / udev 的发布订阅模型:
//   * 订阅方 `subscribe(pattern, handler)` 注册兴趣;返回订阅 id 便于退订;
//   * 发布方 `publish(event_name, payload)` 把 JSON 负载投递给所有匹配的订阅者;
//   * pattern 支持精确匹配、`*`(全匹配)、`dev.*`(前缀多级)、`dev.*.up`(中间段通配);
//   * 单个 Handler 出错只记日志,不影响其他订阅者,也不向 publish 调用方冒泡;
//   * 无订阅者命中时 publish 返回 0,不视为错误。
// ===========================================================================
use anyhow::Result as AnyResult;
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

/// 订阅句柄,由 `Bus::subscribe` 返回,可用于 `Bus::unsubscribe`
pub type SubscriptionId = u64;

/// 事件过滤器:返回 `false` 表示该订阅忽略本次事件。
///
/// 对齐 Linux netlink 的 socket filter / udev 的规则匹配:`Bus` 先按事件名匹配
/// pattern,再依次调用过滤器,只有全部返回 `true` 时才回调内层 `Handler`。
/// 实现必须 `Send + Sync`(订阅表会被多个发布线程并发读取)。
pub trait Filter: Send + Sync {
    /// `event_name` 为事件名(如 `dev.net.up`),`payload` 为 JSON 负载
    fn allow(&self, event_name: &str, payload: &Value) -> bool;
}

/// 任意 `Fn(&str, &Value) -> bool` 闭包都可以直接当过滤器使用
impl<F> Filter for F
where
    F: Fn(&str, &Value) -> bool + Send + Sync,
{
    fn allow(&self, event_name: &str, payload: &Value) -> bool {
        (self)(event_name, payload)
    }
}

/// 便捷构造:`filter_fn(|name, payload| payload["dev"] == "eth0") -> Arc<dyn Filter>`
pub fn filter_fn<F>(f: F) -> Arc<dyn Filter>
where
    F: Fn(&str, &Value) -> bool + Send + Sync + 'static,
{
    Arc::new(f)
}

/// 订阅者接口。模块实现该 trait 后即可挂在总线上。
pub trait Handler: Send + Sync {
    /// `event_name` 为事件名(如 `dev.net.up`),`payload` 为 JSON 负载
    fn handle(&self, event_name: &str, payload: &Value) -> AnyResult<()>;
}

/// 把普通闭包适配成 `Handler`(便于测试与轻量订阅)
pub struct FnHandler<F> {
    f: F,
}

impl<F> FnHandler<F>
where
    F: Fn(&str, &Value) -> AnyResult<()> + Send + Sync,
{
    pub fn new(f: F) -> Self {
        Self { f }
    }
}

impl<F> Handler for FnHandler<F>
where
    F: Fn(&str, &Value) -> AnyResult<()> + Send + Sync,
{
    fn handle(&self, event_name: &str, payload: &Value) -> AnyResult<()> {
        (self.f)(event_name, payload)
    }
}

/// 便捷构造:`handler_fn(|name, payload| { ... }) -> Arc<dyn Handler>`
pub fn handler_fn<F>(f: F) -> Arc<dyn Handler>
where
    F: Fn(&str, &Value) -> AnyResult<()> + Send + Sync + 'static,
{
    Arc::new(FnHandler::new(f))
}

/// 过滤器适配器:包装内层 `Handler` + 一组过滤器,全部放行才真正回调。
///
/// `Bus::subscribe_filtered` 内部就用它把 `(handler, filters)` 一起存进订阅表;
/// 被过滤器拦下的事件静默跳过(返回 `Ok(())`,不算错误,也不影响其他订阅者)。
pub struct FilterHandler {
    inner: Arc<dyn Handler>,
    filters: Vec<Arc<dyn Filter>>,
}

impl FilterHandler {
    pub fn new(inner: Arc<dyn Handler>, filters: Vec<Arc<dyn Filter>>) -> Self {
        Self { inner, filters }
    }

    /// 内层 handler 的引用(便于组合与测试)
    pub fn inner(&self) -> &Arc<dyn Handler> {
        &self.inner
    }

    /// 过滤器数量
    pub fn filter_count(&self) -> usize {
        self.filters.len()
    }

    /// 所有过滤器是否都放行(`all` 短路求值,任一拒绝即停止)
    pub fn allows(&self, event_name: &str, payload: &Value) -> bool {
        self.filters.iter().all(|f| f.allow(event_name, payload))
    }
}

impl Handler for FilterHandler {
    fn handle(&self, event_name: &str, payload: &Value) -> AnyResult<()> {
        if !self.allows(event_name, payload) {
            return Ok(()); // 被过滤:静默跳过
        }
        self.inner.handle(event_name, payload)
    }
}

/// 常用过滤器:要求 `payload[key] == value`(字段缺失视为不匹配)
#[derive(Debug, Clone)]
pub struct PayloadEqFilter {
    pub key: String,
    pub value: Value,
}

impl PayloadEqFilter {
    pub fn new(key: impl Into<String>, value: impl Into<Value>) -> Self {
        Self { key: key.into(), value: value.into() }
    }
}

impl Filter for PayloadEqFilter {
    fn allow(&self, _event_name: &str, payload: &Value) -> bool {
        payload
            .get(self.key.as_str())
            .map(|v| v == &self.value)
            .unwrap_or(false)
    }
}

/// 常用过滤器:只接受 `payload["source"] == module` 的事件(模块间定向通信)
#[derive(Debug, Clone)]
pub struct SourceFilter {
    pub module: String,
}

impl SourceFilter {
    pub fn new(module: impl Into<String>) -> Self {
        Self { module: module.into() }
    }
}

impl Filter for SourceFilter {
    fn allow(&self, _event_name: &str, payload: &Value) -> bool {
        payload.get("source").and_then(Value::as_str) == Some(self.module.as_str())
    }
}

/// 单条订阅记录(pattern + handler)
///
/// 过滤器已由 `FilterHandler` 包进 `handler` 里,这里不再单独保存,
/// 避免同一份过滤逻辑被求值两次。
struct Subscription {
    id: SubscriptionId,
    pattern: String,
    handler: Arc<dyn Handler>,
}

/// 总线共享状态:用 `Arc` 包起来,使 `Bus` 可 clone 并支持 `publish_async`
#[derive(Default)]
struct BusInner {
    subs: Mutex<Vec<Subscription>>,
    next_id: AtomicU64,
    delivered: AtomicU64,
}

/// 内核事件总线
pub struct Bus {
    inner: Arc<BusInner>,
}

impl Clone for Bus {
    fn clone(&self) -> Self {
        Self { inner: Arc::clone(&self.inner) }
    }
}

impl Default for Bus {
    fn default() -> Self {
        Self::new()
    }
}

impl Bus {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BusInner {
                subs: Mutex::new(Vec::new()),
                next_id: AtomicU64::new(0),
                delivered: AtomicU64::new(0),
            }),
        }
    }

    /// 取订阅表锁;Mutex 被 panic 污染时也继续工作(总线不应因单个模块崩溃而失效)
    fn lock_subs(&self) -> std::sync::MutexGuard<'_, Vec<Subscription>> {
        self.inner.subs.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// 订阅 `pattern`,返回订阅 id
    pub fn subscribe(&self, pattern: &str, h: Arc<dyn Handler>) -> SubscriptionId {
        self.subscribe_filtered(pattern, Vec::new(), h)
    }

    /// 带过滤器订阅:pattern 命中 **且** 所有 filter 返回 `true` 时才回调 `h`。
    ///
    /// `filters` 为空等价于 `subscribe`;过滤器按给定顺序求值,任一返回 `false`
    /// 即短路,内层 handler 不会被调用。
    pub fn subscribe_filtered(
        &self,
        pattern: &str,
        filters: Vec<Arc<dyn Filter>>,
        h: Arc<dyn Handler>,
    ) -> SubscriptionId {
        // id 从 1 开始,0 保留给“无效 id”
        let id = self.inner.next_id.fetch_add(1, AtomicOrdering::Relaxed) + 1;
        // 把 h + filters 包成 FilterHandler 存入订阅表
        let handler: Arc<dyn Handler> = if filters.is_empty() {
            h
        } else {
            Arc::new(FilterHandler::new(h, filters))
        };
        self.lock_subs().push(Subscription { id, pattern: pattern.to_string(), handler });
        id
    }

    /// 闭包版订阅(等价于 `subscribe(pattern, handler_fn(f))`)
    pub fn subscribe_fn<F>(&self, pattern: &str, f: F) -> SubscriptionId
    where
        F: Fn(&str, &Value) -> AnyResult<()> + Send + Sync + 'static,
    {
        self.subscribe(pattern, handler_fn(f))
    }

    /// 退订,返回是否真的移除了订阅
    pub fn unsubscribe(&self, id: SubscriptionId) -> bool {
        let mut subs = self.lock_subs();
        let before = subs.len();
        subs.retain(|s| s.id != id);
        subs.len() != before
    }

    /// 按 pattern 批量退订(如模块卸载时清理 `mod.foo.*`),返回移除数量
    pub fn unsubscribe_pattern(&self, pattern: &str) -> usize {
        let mut subs = self.lock_subs();
        let before = subs.len();
        subs.retain(|s| s.pattern != pattern);
        before - subs.len()
    }

    /// 当前订阅者数量
    pub fn subscriber_count(&self) -> usize {
        self.lock_subs().len()
    }

    /// 累计成功投递次数(用于观测/测试)
    pub fn delivered_count(&self) -> u64 {
        self.inner.delivered.load(AtomicOrdering::Relaxed)
    }

    /// 同步发布:遍历匹配订阅并依次调用 `handle`,返回处理成功(`Ok`)的订阅数。
    ///
    /// 事件名匹配在这里完成;字段级过滤由订阅内的 `FilterHandler` 在锁外执行,
    /// 被过滤器拦下的事件静默跳过(该订阅已收到事件,只是主动丢弃)。
    /// 注意:回调前已释放订阅表锁,因此 Handler 内部可以安全地再次 `publish`。
    /// 没有任何订阅命中时返回 `Ok(0)`。
    pub fn publish(&self, event_name: &str, payload: Value) -> AnyResult<usize> {
        let targets: Vec<Arc<dyn Handler>> = {
            let subs = self.lock_subs();
            subs.iter()
                .filter(|s| pattern_matches(&s.pattern, event_name))
                .map(|s| Arc::clone(&s.handler))
                .collect()
        }; // <- 锁在此释放

        let mut delivered = 0usize;
        for h in targets {
            match h.handle(event_name, &payload) {
                Ok(()) => delivered += 1,
                // 单个订阅者失败不影响其他订阅者,仅记日志
                Err(e) => log_error(format!("handler failed on event `{event_name}`: {e}")),
            }
        }
        self.inner
            .delivered
            .fetch_add(delivered as u64, AtomicOrdering::Relaxed);
        Ok(delivered)
    }

    /// JSON 字符串入口:便于从 netlink/配置文件等文本来源直接投递
    pub fn publish_json(&self, event_name: &str, payload_json: &str) -> AnyResult<usize> {
        let payload: Value = serde_json::from_str(payload_json)?;
        self.publish(event_name, payload)
    }

    /// 异步发布:tokio 运行时可用时把投递丢到阻塞线程池,避免拖慢发布方;
    /// 没有运行时(例如纯同步单测)时退化为同步投递,保证事件不丢。
    pub fn publish_async(&self, event_name: &str, payload: Value) -> AnyResult<()> {
        let name = event_name.to_string();
        match tokio::runtime::Handle::try_current() {
            Ok(rt) => {
                let bus = self.clone(); // Arc clone,廉价
                rt.spawn_blocking(move || {
                    if let Err(e) = bus.publish(&name, payload) {
                        log_error(format!("async publish `{name}` failed: {e}"));
                    }
                });
                Ok(())
            }
            Err(_) => self.publish(&name, payload).map(|_| ()),
        }
    }
}

/// 统一错误日志出口。
/// 内核已接入 klog 时,把下面的 `eprintln!` 换成 `crate::klog::warn(msg)` 即可。
#[inline]
fn log_error(msg: impl AsRef<str>) {
    let msg = msg.as_ref();
    // crate::klog::warn(msg);
    eprintln!("[xi::event] {msg}");
}

/// 事件名模式匹配。
///
/// 规则:
/// * 精确匹配:`dev.net.up` 只命中 `dev.net.up`;
/// * `"*"` 命中全部事件;
/// * `dev.*` 命中所有以 `dev.` 开头的多级事件(`dev.net`, `dev.net.up`, ...);
/// * 按 `.` 分段逐段比较,`*` 段匹配任意**单**段(`dev.*.up` 命中 `dev.net.up`,不命中 `dev.a.b.up`)。
pub fn pattern_matches(pattern: &str, event_name: &str) -> bool {
    // 1) 全通配 / 精确匹配
    if pattern == "*" || pattern == event_name {
        return true;
    }
    if !pattern.contains('*') {
        return false;
    }

    // 2) 结尾 `.*` 视为前缀匹配,支持多级(dev.* -> dev.net.up)
    if let Some(prefix) = pattern.strip_suffix(".*") {
        if !prefix.contains('*') {
            let bytes = event_name.as_bytes();
            return event_name.len() > prefix.len() + 1
                && event_name.starts_with(prefix)
                && bytes[prefix.len()] == b'.';
        }
    }

    // 3) 逐段比较(段数必须一致)
    let mut pat_segs = pattern.split('.');
    let mut evt_segs = event_name.split('.');
    loop {
        match (pat_segs.next(), evt_segs.next()) {
            (None, None) => return true,
            (Some("*"), Some(_)) => continue,
            (Some(a), Some(b)) if a == b => continue,
            _ => return false,
        }
    }
}

/// 是否包含通配符(供上层做优化判断)
pub fn has_wildcard(pattern: &str) -> bool {
    pattern.contains('*')
}

#[cfg(test)]
mod bus_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 记录命中次数的测试 Handler
    struct Counter {
        hits: AtomicUsize,
        last: Mutex<Option<(String, Value)>>,
    }

    impl Counter {
        fn new() -> Arc<Self> {
            Arc::new(Self { hits: AtomicUsize::new(0), last: Mutex::new(None) })
        }
        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }
    }

    impl Handler for Counter {
        fn handle(&self, event_name: &str, payload: &Value) -> AnyResult<()> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            *self.last.lock().unwrap() = Some((event_name.to_string(), payload.clone()));
            Ok(())
        }
    }

    fn payload() -> Value {
        serde_json::json!({ "dev": "eth0", "ts": 1234u64 })
    }

    #[test]
    fn test_pattern_matches_rules() {
        // 精确
        assert!(pattern_matches("dev.net.up", "dev.net.up"));
        assert!(!pattern_matches("dev.net.up", "dev.net.down"));
        // 全通配
        assert!(pattern_matches("*", "anything.at.all"));
        assert!(pattern_matches("*", "heartbeat"));
        // 前缀多级
        assert!(pattern_matches("dev.*", "dev.net"));
        assert!(pattern_matches("dev.*", "dev.net.up"));
        assert!(!pattern_matches("dev.*", "dev"));
        assert!(!pattern_matches("dev.*", "devnet.up"));
        // 中间段通配
        assert!(pattern_matches("dev.*.up", "dev.eth0.up"));
        assert!(!pattern_matches("dev.*.up", "dev.a.b.up"));
        assert!(!pattern_matches("dev.*.up", "dev.eth0.down"));
        // 无通配符且不相等
        assert!(!pattern_matches("dev.net", "dev.net.up"));
    }

    #[test]
    fn test_publish_then_subscribe_receives() {
        let bus = Bus::new();
        let h = Counter::new();
        let id = bus.subscribe("dev.net.up", h.clone());
        assert_eq!(id, 1);
        assert_eq!(bus.subscriber_count(), 1);

        let n = bus.publish("dev.net.up", payload()).unwrap();
        assert_eq!(n, 1);
        assert_eq!(h.hits(), 1);

        let (name, body) = h.last.lock().unwrap().clone().unwrap();
        assert_eq!(name, "dev.net.up");
        assert_eq!(body["dev"], "eth0");
        assert_eq!(bus.delivered_count(), 1);
    }

    #[test]
    fn test_wildcard_subscription() {
        let bus = Bus::new();
        let star = Counter::new();
        let prefix = Counter::new();
        let exact = Counter::new();
        bus.subscribe("*", star.clone());
        bus.subscribe("dev.*", prefix.clone());
        bus.subscribe("dev.net.down", exact.clone());

        // 命中 star + dev.*
        assert_eq!(bus.publish("dev.net.up", payload()).unwrap(), 2);
        assert_eq!(star.hits(), 1);
        assert_eq!(prefix.hits(), 1);
        assert_eq!(exact.hits(), 0);

        // 命中全部三个
        assert_eq!(bus.publish("dev.net.down", payload()).unwrap(), 3);
        assert_eq!(exact.hits(), 1);

        // 与 dev.* 无关的事件只命中 star
        assert_eq!(bus.publish("task.started", payload()).unwrap(), 1);
        assert_eq!(star.hits(), 3);
    }

    #[test]
    fn test_filter_blocks_delivery() {
        let bus = Bus::new();
        let eth0 = Counter::new();
        let other = Counter::new();
        // 只关心 dev == eth0 的事件
        bus.subscribe_filtered(
            "dev.*",
            vec![Arc::new(PayloadEqFilter::new("dev", "eth0"))],
            eth0.clone(),
        );
        bus.subscribe("dev.*", other.clone());

        bus.publish("dev.net.up", serde_json::json!({ "dev": "eth0" })).unwrap();
        assert_eq!(eth0.hits(), 1);
        assert_eq!(other.hits(), 1);

        // 过滤器拒绝,但无过滤器的订阅仍收到
        bus.publish("dev.net.up", serde_json::json!({ "dev": "wlan0" })).unwrap();
        assert_eq!(eth0.hits(), 1);
        assert_eq!(other.hits(), 2);
    }

    #[test]
    fn test_multiple_filters_must_all_pass() {
        let bus = Bus::new();
        let hit = Counter::new();
        // 两个过滤器必须同时满足:source == "net" 且 dev == "eth0"
        bus.subscribe_filtered(
            "dev.*",
            vec![
                Arc::new(SourceFilter::new("net")),
                Arc::new(PayloadEqFilter::new("dev", "eth0")),
            ],
            hit.clone(),
        );

        bus.publish("dev.net.up", serde_json::json!({ "source": "net", "dev": "eth0" }))
            .unwrap();
        assert_eq!(hit.hits(), 1);

        // source 不匹配
        bus.publish("dev.net.up", serde_json::json!({ "source": "fs", "dev": "eth0" }))
            .unwrap();
        // dev 不匹配
        bus.publish("dev.net.up", serde_json::json!({ "source": "net", "dev": "wlan0" }))
            .unwrap();
        // source 字段缺失 -> SourceFilter 拒绝
        bus.publish("dev.net.up", serde_json::json!({ "dev": "eth0" })).unwrap();
        assert_eq!(hit.hits(), 1);
    }

    #[test]
    fn test_source_filter_and_closure_filter() {
        let bus = Bus::new();
        let from_net = Counter::new();
        bus.subscribe_filtered("*", vec![Arc::new(SourceFilter::new("net"))], from_net.clone());

        // 闭包也能当过滤器(见 `Filter` 的 blanket impl / `filter_fn`)
        let big_ts = Counter::new();
        bus.subscribe_filtered(
            "*",
            vec![filter_fn(|_: &str, p: &Value| p["ts"].as_u64().unwrap_or(0) > 1000)],
            big_ts.clone(),
        );

        bus.publish("dev.net.up", serde_json::json!({ "source": "net", "ts": 5 })).unwrap();
        assert_eq!(from_net.hits(), 1);
        assert_eq!(big_ts.hits(), 0);

        bus.publish("dev.net.up", serde_json::json!({ "source": "fs", "ts": 2000 }))
            .unwrap();
        assert_eq!(from_net.hits(), 1);
        assert_eq!(big_ts.hits(), 1);
    }

    #[test]
    fn test_filter_handler_composition() {
        let bus = Bus::new();
        let counter = Counter::new();
        // 手动组合 FilterHandler,再通过普通 subscribe 挂上去
        let wrapped: Arc<dyn Handler> = Arc::new(FilterHandler::new(
            counter.clone(),
            vec![Arc::new(PayloadEqFilter::new("ok", true))],
        ));
        bus.subscribe("*", wrapped);

        bus.publish("dev.net.up", serde_json::json!({ "ok": false })).unwrap();
        assert_eq!(counter.hits(), 0);
        bus.publish("dev.net.up", serde_json::json!({ "ok": true })).unwrap();
        assert_eq!(counter.hits(), 1);
    }

    #[test]
    fn test_unsubscribe_filtered() {
        let bus = Bus::new();
        let h = Counter::new();
        let id = bus.subscribe_filtered(
            "dev.*",
            vec![Arc::new(SourceFilter::new("net"))],
            h.clone(),
        );
        assert_eq!(bus.subscriber_count(), 1);
        assert!(bus.unsubscribe(id));
        bus.publish("dev.net.up", serde_json::json!({ "source": "net" })).unwrap();
        assert_eq!(h.hits(), 0);
    }

    #[test]
    fn test_unsubscribe() {
        let bus = Bus::new();
        let h = Counter::new();
        let id = bus.subscribe("dev.*", h.clone());
        assert!(bus.unsubscribe(id));
        assert!(!bus.unsubscribe(id)); // 重复退订返回 false
        assert_eq!(bus.subscriber_count(), 0);

        // 未命中任何订阅:返回 0,不报错
        assert_eq!(bus.publish("dev.net.up", payload()).unwrap(), 0);
        assert_eq!(h.hits(), 0);
    }

    #[test]
    fn test_handler_error_isolated() {
        let bus = Bus::new();
        let bad = Counter::new();
        let good = Counter::new();
        bus.subscribe("*", handler_fn(|_, _| anyhow::bail!("boom")));
        bus.subscribe("dev.*", bad.clone());
        bus.subscribe("dev.net.up", good.clone());

        // 失败的那个不计入投递数,其余仍正常收到
        assert_eq!(bus.publish("dev.net.up", payload()).unwrap(), 2);
        assert_eq!(bad.hits(), 1);
        assert_eq!(good.hits(), 1);
    }

    #[test]
    fn test_publish_async_without_runtime_falls_back_sync() {
        // `#[test]` 里没有 tokio 运行时 -> 同步退化路径,事件立即送达
        let bus = Bus::new();
        let h = Counter::new();
        bus.subscribe("dev.*", h.clone());
        bus.publish_async("dev.net.up", payload()).unwrap();
        assert_eq!(h.hits(), 1);
    }

    #[test]
    fn test_publish_json_and_reentrant_publish() {
        let bus = Bus::new();
        // 在 Handler 内部再次 publish,验证不会因持锁回调而死锁
        let inner = bus.clone();
        bus.subscribe_fn("dev.net.up", move |_, _| {
            inner.publish("dev.net.logged", serde_json::json!({ "ok": true }))?;
            Ok(())
        });
        let tail = Counter::new();
        bus.subscribe("dev.net.logged", tail.clone());

        assert_eq!(bus.publish_json("dev.net.up", "{\"dev\":\"eth0\"}").unwrap(), 1);
        assert_eq!(tail.hits(), 1);
        assert_eq!(tail.last.lock().unwrap().clone().unwrap().1["ok"], true);
    }
}

// ===========================================================================
// 步骤 5 单测:发布/订阅、通配符、过滤器、异步发布、退订
// ===========================================================================
#[cfg(test)]
mod bus_step5_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Mutex;
    use std::time::Duration;

    /// 计数 + 记录最近一次 `(event_name, payload)` 的测试 Handler
    struct Recorder {
        hits: AtomicUsize,
        last_name: Mutex<Option<String>>,
        last_payload: Mutex<Option<Value>>,
    }

    impl Recorder {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                hits: AtomicUsize::new(0),
                last_name: Mutex::new(None),
                last_payload: Mutex::new(None),
            })
        }

        fn hits(&self) -> usize {
            self.hits.load(Ordering::SeqCst)
        }

        /// 最近一次收到的事件(未收到返回 None)
        fn last(&self) -> Option<(String, Value)> {
            let name = self.last_name.lock().unwrap().clone()?;
            let payload = self.last_payload.lock().unwrap().clone()?;
            Some((name, payload))
        }
    }

    impl Handler for Recorder {
        fn handle(&self, event_name: &str, payload: &Value) -> AnyResult<()> {
            self.hits.fetch_add(1, Ordering::SeqCst);
            *self.last_name.lock().unwrap() = Some(event_name.to_string());
            *self.last_payload.lock().unwrap() = Some(payload.clone());
            Ok(())
        }
    }

    /// 1) 精确订阅:注册 `dev.net.up`,发布同名事件,收到且 payload 一致,投递数 1
    #[test]
    fn test_publish_subscribe() {
        let bus = Bus::new();
        let h = Recorder::new();

        let id = bus.subscribe("dev.net.up", h.clone());
        assert!(id > 0, "订阅 id 应从 1 开始");
        assert_eq!(bus.subscriber_count(), 1);

        let payload = serde_json::json!({ "dev": "eth0", "ts": 1234u64 });
        let delivered = bus.publish("dev.net.up", payload.clone()).unwrap();

        // 投递数 = 1,handler 被调用一次
        assert_eq!(delivered, 1);
        assert_eq!(h.hits(), 1);

        // payload 一致,事件名不被篡改
        let (name, body) = h.last().expect("handler 应收到事件");
        assert_eq!(name, "dev.net.up");
        assert_eq!(body, payload);
        assert_eq!(bus.delivered_count(), 1);

        // 名字不同的事件不触发该订阅
        assert_eq!(bus.publish("dev.net.down", payload).unwrap(), 0);
        assert_eq!(h.hits(), 1);
    }

    /// 2) 通配符:`*`、`dev.*`、`dev.*.up` 的命中集合符合预期
    #[test]
    fn test_wildcard() {
        let bus = Bus::new();

        // 用闭包订阅把收到的事件名收集起来,便于按集合断言
        let star = Arc::new(Mutex::new(Vec::<String>::new()));
        let prefix = Arc::new(Mutex::new(Vec::<String>::new()));
        let middle = Arc::new(Mutex::new(Vec::<String>::new()));

        let (s, p, m) = (star.clone(), prefix.clone(), middle.clone());
        bus.subscribe_fn("*", move |name, _| {
            s.lock().unwrap().push(name.to_string());
            Ok(())
        });
        bus.subscribe_fn("dev.*", move |name, _| {
            p.lock().unwrap().push(name.to_string());
            Ok(())
        });
        bus.subscribe_fn("dev.*.up", move |name, _| {
            m.lock().unwrap().push(name.to_string());
            Ok(())
        });

        // 依次发布,并校验每次的投递数(matcher 命中数)
        let cases: &[(&str, usize)] = &[
            ("dev.net.up", 3),    // * + dev.* + dev.*.up
            ("dev.net.down", 2),  // * + dev.*
            ("dev.eth0.up", 3),   // * + dev.* + dev.*.up
            ("task.started", 1),  // 仅 *
            ("heartbeat", 1),     // 仅 *
        ];
        for (name, expected) in cases {
            let n = bus.publish(name, serde_json::json!({ "dev": "eth0" })).unwrap();
            assert_eq!(n, *expected, "事件 `{name}` 的投递数不符合预期");
        }

        let got = |v: &Arc<Mutex<Vec<String>>>| v.lock().unwrap().clone();
        assert_eq!(
            got(&star),
            vec!["dev.net.up", "dev.net.down", "dev.eth0.up", "task.started", "heartbeat"]
        );
        assert_eq!(got(&prefix), vec!["dev.net.up", "dev.net.down", "dev.eth0.up"]);
        assert_eq!(got(&middle), vec!["dev.net.up", "dev.eth0.up"]);

        // 直接断言 pattern_matches 的多组用例
        assert!(pattern_matches("*", "dev.net.up"));
        assert!(pattern_matches("*", "heartbeat"));
        assert!(pattern_matches("dev.*", "dev.net"));
        assert!(pattern_matches("dev.*", "dev.net.up"));
        assert!(!pattern_matches("dev.*", "dev"));
        assert!(!pattern_matches("dev.*", "devnet.up"));
        assert!(pattern_matches("dev.*.up", "dev.eth0.up"));
        assert!(!pattern_matches("dev.*.up", "dev.a.b.up"));
        assert!(!pattern_matches("dev.*.up", "dev.eth0.down"));
        assert!(pattern_matches("dev.net.up", "dev.net.up"));
        assert!(!pattern_matches("dev.net", "dev.net.up"));
    }

    /// 3) 过滤器:只有 source == kernel 且 level == err 的 payload 才触发 handler
    #[test]
    fn test_filter() {
        let bus = Bus::new();
        let filtered = Recorder::new();

        bus.subscribe_filtered(
            "kernel.*",
            vec![
                Arc::new(SourceFilter::new("kernel")),
                Arc::new(PayloadEqFilter::new("level", "err")),
            ],
            filtered.clone(),
        );
        // 无过滤器的对照订阅,确认事件确实被发布出去了
        let raw = Recorder::new();
        bus.subscribe("kernel.*", raw.clone());

        // 全部条件满足 —— 唯一一次触发
        bus.publish(
            "kernel.log",
            serde_json::json!({ "source": "kernel", "level": "err", "msg": "oops" }),
        )
        .unwrap();
        assert_eq!(filtered.hits(), 1);
        assert_eq!(raw.hits(), 1);

        // source 不匹配 —— 被 SourceFilter 拦下
        bus.publish("kernel.log", serde_json::json!({ "source": "user", "level": "err" }))
            .unwrap();
        assert_eq!(filtered.hits(), 1);
        assert_eq!(raw.hits(), 2);

        // level 不匹配 —— 被 PayloadEqFilter 拦下
        bus.publish("kernel.log", serde_json::json!({ "source": "kernel", "level": "warn" }))
            .unwrap();
        assert_eq!(filtered.hits(), 1);
        assert_eq!(raw.hits(), 3);

        // source 缺失 —— SourceFilter 拒绝
        bus.publish("kernel.log", serde_json::json!({ "level": "err" })).unwrap();
        assert_eq!(filtered.hits(), 1);
        assert_eq!(raw.hits(), 4);

        // 事件名不匹配 pattern 时两边都不触发
        bus.publish(
            "kernel.log",
            serde_json::json!({ "source": "kernel", "level": "err" }),
        )
        .unwrap();
        assert_eq!(filtered.hits(), 2, "第二次满足全部条件的事件应再次触发");
    }

    /// 4) 异步发布:tokio 运行时下 `publish_async` 最终仍会调用一次 handler
    ///
    /// 需要 tokio 开启 `macros` + `rt`(`spawn_blocking` 依赖 rt)。
    #[tokio::test]
    async fn test_async_publish() {
        let bus = Bus::new();
        let h = Recorder::new();
        bus.subscribe("dev.net.up", h.clone());

        bus.publish_async("dev.net.up", serde_json::json!({ "dev": "eth0" }))
            .unwrap();
        // 发布是立即返回的,投递发生在阻塞线程池中
        assert_eq!(h.hits(), 0, "publish_async 不应同步回调 handler");

        // 轮询等待(用 std 睡眠,避免依赖 tokio `time` feature)
        for _ in 0..200 {
            if h.hits() > 0 {
                break;
            }
            std::thread::sleep(Duration::from_millis(5));
        }

        assert_eq!(h.hits(), 1, "异步投递应恰好调用 handler 一次");
        let (name, body) = h.last().expect("handler 应收到异步事件");
        assert_eq!(name, "dev.net.up");
        assert_eq!(body["dev"], "eth0");
        assert_eq!(bus.delivered_count(), 1);
    }

    /// 5) 退订:取消订阅后 publish 返回 0
    #[test]
    fn test_unsubscribe() {
        let bus = Bus::new();
        let h = Recorder::new();
        let id = bus.subscribe("dev.net.up", h.clone());

        assert_eq!(bus.publish("dev.net.up", serde_json::json!({})).unwrap(), 1);
        assert_eq!(h.hits(), 1);

        assert!(bus.unsubscribe(id), "退订已存在的 id 应返回 true");
        assert_eq!(bus.subscriber_count(), 0);

        let n = bus.publish("dev.net.up", serde_json::json!({})).unwrap();
        assert_eq!(n, 0, "退订后投递数应为 0");
        assert_eq!(h.hits(), 1, "退订后 handler 不应再被调用");

        // 重复退订同一 id 返回 false
        assert!(!bus.unsubscribe(id));

        // 按 pattern 退订
        bus.subscribe("mod.foo.*", Recorder::new());
        bus.subscribe("mod.foo.*", Recorder::new());
        assert_eq!(bus.unsubscribe_pattern("mod.foo.*"), 2);
        assert_eq!(bus.subscriber_count(), 0);
        assert_eq!(bus.publish("mod.foo.loaded", serde_json::json!({})).unwrap(), 0);
    }
}