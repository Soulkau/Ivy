use core::fmt::Write;
use core::sync::atomic::{AtomicU32, Ordering};

use embassy_sync::blocking_mutex::raw::{CriticalSectionRawMutex, RawMutex};
use embassy_sync::channel::{Channel, Sender};
use heapless::String;
use static_cell::StaticCell;
use talky::logs::{DeviceLog, LogLevel};
use tracing::{
    Dispatch, Level, Subscriber,
    field::{Field, Visit},
    span,
};

const DEFAULT_LOG_SIZE: usize = 512;

const DEFAULT_TAG_SIZE: usize = 16;

const DEFAULT_LOG_QUEUE_SIZE: usize = 16;

type DefaultLog = DeviceLog<DEFAULT_LOG_SIZE, DEFAULT_TAG_SIZE>;

pub trait LogEmitter<const S: usize, const T: usize>: Sync + Send {
    fn send(&self, device_log: DeviceLog<S, T>);
}

pub trait LogSink<const S: usize, const T: usize> {
    async fn next(&self) -> DeviceLog<S, T>;
}

pub struct Logger<const S: usize, const T: usize, E: LogEmitter<S, T>> {
    max_level: Level,
    next_id: AtomicU32,
    emitter: E,
}

impl<const S: usize, const T: usize, E: LogEmitter<S, T>> Logger<S, T, E> {
    pub fn new(max_level: Level, emitter: E) -> Self {
        Self {
            max_level,
            next_id: AtomicU32::new(1),
            emitter,
        }
    }
}

impl<const S: usize, const T: usize, E: LogEmitter<S, T> + 'static> Subscriber for Logger<S, T, E> {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.level() <= &self.max_level
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = BufVisitor::new();
        event.record(&mut visitor);

        let log = DeviceLog::new(LogLevel::from(event.metadata().level()), visitor.buf, visitor.tag);
        self.emitter.send(log);
    }

    fn new_span(&self, span: &span::Attributes<'_>) -> span::Id {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        span::Id::from_u64(id as u64)
    }

    fn record(&self, span: &span::Id, values: &span::Record<'_>) {}

    fn exit(&self, span: &span::Id) {}

    fn enter(&self, span: &span::Id) {}

    fn record_follows_from(&self, span: &span::Id, follows: &span::Id) {}
}

pub struct BufVisitor<const S: usize, const T: usize> {
    pub buf: String<S>,
    pub tag: String<T>,
}

impl<const S: usize, const T: usize> BufVisitor<S, T> {
    pub fn new() -> Self {
        Self {
            buf: String::new(),
            tag: String::new(),
        }
    }

    // core::fmt::Write returns Err on overflow for heapless::String,
    // we just eat the error and keep whatever fit.
    fn write(&mut self, args: core::fmt::Arguments) {
        let _ = self.buf.write_fmt(args);
    }
}

impl<const S: usize, const T: usize> Visit for BufVisitor<S, T> {
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

impl<'a, const S: usize, const T: usize, M: RawMutex + Sync, const Q: usize> LogEmitter<S, T> for Sender<'a, M, DeviceLog<S, T>, Q> {
    fn send(&self, device_log: DeviceLog<S, T>) {
        self.try_send(device_log);
    }
}

impl<const S: usize, const T: usize, M: RawMutex + Sync, const Q: usize> LogSink<S, T> for Channel<M, DeviceLog<S, T>, Q> {
    async fn next(&self) -> DeviceLog<S, T> {
        self.receive().await
    }
}

fn register_dispatcher<E: LogEmitter<S, T> + 'static, const S: usize, const T: usize>(level: Level, sink: E) {
    tracing::dispatcher::set_global_default(Dispatch::new(Logger::new(level, sink))).unwrap();
}

pub fn init_default_logger(level: Level) -> &'static Channel<CriticalSectionRawMutex, DefaultLog, DEFAULT_LOG_QUEUE_SIZE> {
    static CHANNEL: StaticCell<Channel<CriticalSectionRawMutex, DefaultLog, DEFAULT_LOG_QUEUE_SIZE>> = StaticCell::new();
    let channel = &*CHANNEL.init_with(|| Channel::new());
    register_dispatcher(level, channel.sender());
    channel
}
