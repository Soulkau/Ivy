use core::fmt::Write;
use core::sync::atomic::{AtomicU32, Ordering};

use embassy_sync::blocking_mutex::raw::{CriticalSectionRawMutex, RawMutex};
use embassy_sync::channel::{Channel, Receiver, Sender};
use heapless::String;
use static_cell::StaticCell;
use talky::logs::{DeviceLog, LogLevel};
use tracing::{
    Dispatch, Level, Subscriber,
    field::{Field, Visit},
    span,
};

pub const LOG_SIZE: usize = 512;
pub const TAG_SIZE: usize = 16;
pub const LOG_QUEUE_SIZE: usize = 16;

pub type Log = DeviceLog<LOG_SIZE, TAG_SIZE>;

pub type LogSink = Receiver<'static, CriticalSectionRawMutex, Log, LOG_QUEUE_SIZE>;

pub trait LogEmitter: Sync + Send {
    fn send(&self, device_log: Log);
}

#[allow(async_fn_in_trait)]
pub trait LogConsumer {
    async fn consume_logs(&mut self, sink: LogSink) -> !;
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

    // core::fmt::Write returns Err on overflow for heapless::String,
    // we just eat the error and keep whatever fit.
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

impl<'a, M: RawMutex + Sync, const Q: usize> LogEmitter for Sender<'a, M, Log, Q> {
    fn send(&self, device_log: Log) {
        self.try_send(device_log).ok();
    }
}

fn register_dispatcher<E: LogEmitter + 'static>(level: Level, sink: E) {
    tracing::dispatcher::set_global_default(Dispatch::new(Logger::new(level, sink))).unwrap();
}

pub fn init_subscriber(level: Level) -> LogSink {
    static CHANNEL: StaticCell<Channel<CriticalSectionRawMutex, Log, LOG_QUEUE_SIZE>> = StaticCell::new();
    let channel = &*CHANNEL.init_with(|| Channel::new());
    register_dispatcher(level, channel.sender());
    channel.receiver()
}

#[macro_export]
macro_rules! init_logger {
    ($spawner:expr, $consumer:expr, $sink:expr, $consumer_ty:ty) => {{
        #[embassy_executor::task]
        async fn ___logger_task(mut consumer: $consumer_ty, sink: $crate::logger::LogSink) -> ! {
            use $crate::logger::LogConsumer;
            consumer.consume_logs(sink).await
        }
        $spawner.spawn(___logger_task($consumer, $sink).unwrap());
    }};
}
