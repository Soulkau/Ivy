use core::cell::RefCell;
use core::fmt::Write;
use core::sync::atomic::{AtomicU32, Ordering};

use embassy_sync::blocking_mutex::Mutex;
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_sync::signal::Signal;
use heapless::{Deque, String};
use static_cell::StaticCell;
use talky::logs::{DeviceLog, LogLevel};
use tracing::{
    Dispatch, Level, Subscriber,
    field::{Field, Visit},
    span,
};

pub const LOG_SIZE: usize = 256;
pub const TAG_SIZE: usize = 16;
pub const LOG_QUEUE_SIZE: usize = 16;

pub type Log = DeviceLog<LOG_SIZE, TAG_SIZE>;
pub type SizedLogQueue = LogQueue<LOG_QUEUE_SIZE>;

// the consumer just needs a 'static ref into the queue to pop() from
pub type LogSink = &'static SizedLogQueue;

pub trait LogEmitter: Sync + Send {
    fn send(&self, log: Log);
}

#[allow(async_fn_in_trait)]
pub trait LogConsumer {
    async fn consume_logs(&mut self, sink: LogSink) -> !;
}

pub struct LogQueue<const N: usize> {
    buf: Mutex<CriticalSectionRawMutex, RefCell<Deque<Log, N>>>,
    notify: Signal<CriticalSectionRawMutex, ()>,
}

impl<const N: usize> LogQueue<N> {
    pub const fn new() -> Self {
        Self {
            buf: Mutex::new(RefCell::new(Deque::new())),
            notify: Signal::new(),
        }
    }

    pub fn push(&self, item: Log) {
        self.buf.lock(|cell| {
            let mut buf = cell.borrow_mut();
            if buf.is_full() {
                buf.pop_front();
            }
            buf.push_back(item).ok();
        });
        self.notify.signal(());
    }

    pub async fn pop(&self) -> Log {
        loop {
            let item = self.buf.lock(|cell| cell.borrow_mut().pop_front());
            if let Some(item) = item {
                return item;
            }
            self.notify.wait().await;
        }
    }
}

// emitter impl is on the *reference*, since Logger owns E by value
// and we need Logger to hold a handle into the static queue, not the queue itself
impl<const N: usize> LogEmitter for &'static LogQueue<N> {
    fn send(&self, log: Log) {
        LogQueue::push(self, log);
    }
}

pub struct Logger<E: LogEmitter> {
    max_level: Level,
    next_id: AtomicU32,
    emitter: E,
}

impl<E: LogEmitter> Logger<E> {
    pub fn new(max_level: Level, emitter: E) -> Self {
        Self {
            max_level,
            next_id: AtomicU32::new(1),
            emitter,
        }
    }
}

impl<E: LogEmitter + 'static> Subscriber for Logger<E> {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.level() <= &self.max_level
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = BufVisitor::new();
        event.record(&mut visitor);

        let log = DeviceLog::new(LogLevel::from(event.metadata().level()), visitor.buf, visitor.tag);
        self.emitter.send(log);
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        span::Id::from_u64(id as u64)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}
    fn exit(&self, _: &span::Id) {}
    fn enter(&self, _: &span::Id) {}
    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}
}

pub struct BufVisitor {
    pub buf: String<LOG_SIZE>,
    pub tag: String<TAG_SIZE>,
}

impl BufVisitor {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            tag: "notag".try_into().unwrap(),
        }
    }

    fn write(&mut self, args: core::fmt::Arguments) {
        let _ = self.buf.write_fmt(args);
    }
}

impl Visit for BufVisitor {
    fn record_debug(&mut self, field: &Field, value: &dyn core::fmt::Debug) {
        if field.name() == "message" {
            self.write(format_args!("{:?}", value));
        } else {
            self.write(format_args!(" {}={:?}", field.name(), value));
        }
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        if field.name() == "tag" {
            if let Ok(t) = String::try_from(value) {
                self.tag = t;
            }
        } else if field.name() == "message" {
            self.write(format_args!("{}", value));
        } else {
            self.write(format_args!(" {}={}", field.name(), value));
        }
    }

    fn record_i64(&mut self, field: &Field, value: i64) {
        self.write(format_args!(" {}={}", field.name(), value));
    }

    fn record_u64(&mut self, field: &Field, value: u64) {
        self.write(format_args!(" {}={}", field.name(), value));
    }

    fn record_bool(&mut self, field: &Field, value: bool) {
        self.write(format_args!(" {}={}", field.name(), value));
    }
}

fn register_dispatcher(level: Level, queue: LogSink) {
    tracing::dispatcher::set_global_default(Dispatch::new(Logger::new(level, queue))).unwrap();
}

pub fn init_subscriber(level: Level) -> LogSink {
    static QUEUE: StaticCell<SizedLogQueue> = StaticCell::new();
    let queue: LogSink = QUEUE.init_with(LogQueue::new);
    register_dispatcher(level, queue);
    queue
}

#[macro_export]
macro_rules! init_logger {
    ($spawner:expr, $consumer:expr, $sink:expr, $consumer_ty:ty) => {{
        #[embassy_executor::task]
        async fn ___logger_task(mut consumer: $consumer_ty, sink: $crate::logger::LogSink) -> ! {
            use $crate::logger::LogConsumer;
            consumer.consume_logs(sink).await
        }
        //TODO: error handling, do not panic
        $spawner.spawn(___logger_task($consumer, $sink).expect("Failed to run log task"));
    }};
}
