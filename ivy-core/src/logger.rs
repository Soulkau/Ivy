use core::sync::atomic::{AtomicU32, Ordering};

use embassy_sync::{
    blocking_mutex::raw::{CriticalSectionRawMutex, NoopRawMutex},
    channel::Channel,
};
use talky::logs::{BufVisitor, DeviceLog, LogLevel};
use tracing::{Dispatch, Level, Subscriber, span};

pub static LOG_CHANNEL: Channel<CriticalSectionRawMutex, DeviceLog, 16> = Channel::new();

pub fn init_mqtt_logger() {
    tracing::dispatcher::set_global_default(Dispatch::new(SendLogger::new(Level::DEBUG, &LOG_CHANNEL))).unwrap();
}

pub trait LogSink: Sync {
    fn try_send(&self, log: DeviceLog);
}

impl<const N: usize> LogSink for Channel<CriticalSectionRawMutex, DeviceLog, N> {
    fn try_send(&self, log: DeviceLog) {
        let _ = Channel::try_send(self, log);
    }
}

pub struct SendLogger {
    max_level: Level,
    sink: &'static dyn LogSink,
    next_id: AtomicU32,
}

impl SendLogger {
    pub fn new(max_level: Level, sink: &'static dyn LogSink) -> Self {
        Self {
            max_level,
            sink,
            next_id: AtomicU32::new(1),
        }
    }
}

impl Subscriber for SendLogger {
    fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
        metadata.level() <= &self.max_level
    }

    fn event(&self, event: &tracing::Event<'_>) {
        let mut visitor = BufVisitor::new();
        event.record(&mut visitor);

        let log = DeviceLog::new(LogLevel::from(event.metadata().level()), visitor.buf);
        self.sink.try_send(log);
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
