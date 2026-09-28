use core::net::Ipv4Addr;

use alloc::boxed::Box;
use core::alloc::Allocator;

use embassy_futures::{
    join::join,
    select::{select, select3},
};
use embassy_net::{
    Stack,
    tcp::client::{TcpClient, TcpClientState},
};
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, signal::Signal};
use embassy_time::{Duration, Timer};
use embedded_tls::{Aes128GcmSha256, CryptoRngCore, TlsConfig, UnsecureProvider};
use heapless::String;
use ivy_macros::actor_handle;
use ivy_types::actor::{Actor, rt::launder_slice};
use mqttrust::{
    Config, IpBroker, MqttClient, MqttStack, Publish, State, Subscribe, SubscribeTopic,
    transport::embedded_tls::{TlsNalTransport, TlsState},
};
use serde::{Serialize, de::DeserializeOwned};

pub use mqttrust::State as MqttState;
use static_cell::StaticCell;

use crate::logger::{LOG_SIZE, LogConsumer, LogSink, TAG_SIZE};

pub type MqttTcpClientState<const TCP: usize> = TcpClientState<1, TCP, TCP>;
pub type MqttTcpClient<const TCP: usize> = TcpClient<'static, 1, TCP, TCP>;
pub type MqttTlsState<const TLS: usize> = TlsState<TLS, TLS>;
pub type MqttTlsTransport<Rng, const TCP: usize, const TLS: usize> = TlsNalTransport<'static, MqttTcpClient<TCP>, IpBroker, MqttProvider<Rng>, TLS, TLS>;
type MqttProvider<Rng> = UnsecureProvider<'static, Aes128GcmSha256, Rng>;

#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("decode error: {0}")]
    Decode(#[from] serde_json_core::de::Error),
    #[error("encode error: {0}")]
    Encode(#[from] serde_json_core::ser::Error),
    #[error("mqtt client error: {0:?}")]
    MqttClient(mqttrust::Error),
    #[error("Disconnected")]
    Disconnected,
}

pub struct Subscription<T: 'static> {
    receiver: &'static Signal<CriticalSectionRawMutex, T>,
}

impl<T: 'static> Subscription<T> {
    pub fn new(receiver: &'static Signal<CriticalSectionRawMutex, T>) -> Self {
        Self { receiver }
    }
}

impl<T: 'static> Subscription<T> {
    pub async fn next(&self) -> T {
        self.receiver.wait().await
    }
}

pub trait ErasedHandle: Send + Sync {
    fn dispatch(&self, buf: &[u8]) -> Result<(), MqttError>;
}

pub struct TypedHandle<T: 'static> {
    signal: &'static Signal<CriticalSectionRawMutex, T>,
}

impl<T: 'static> TypedHandle<T> {
    pub fn new(signal: &'static Signal<CriticalSectionRawMutex, T>) -> Self {
        Self { signal }
    }
}

/* SAFETY: DynamicSender only exposes try_send, which goes through
the channel's internal critical-section mutex - it's fine to
call from multiple threads concurrently as long as T: Send. */
unsafe impl<T: Send + 'static> Sync for TypedHandle<T> {}
unsafe impl<T: Send + 'static> Send for TypedHandle<T> {}

impl<T> ErasedHandle for TypedHandle<T>
where
    T: DeserializeOwned + Send + Sync + 'static,
{
    fn dispatch(&self, buf: &[u8]) -> Result<(), MqttError> {
        let (value, _) = serde_json_core::from_slice(buf).map_err(MqttError::Decode)?;
        self.signal.signal(value);
        Ok(())
    }
}

#[actor_handle(RawMqttHandle)]
pub trait MqttHandle {
    async fn __publish(&self, topic: &'static str, payload: &'static [u8]) -> Result<(), MqttError>;
}

impl RawMqttHandle {
    /// # Safety
    /// `payload` must stay valid until the actor finishes reading it (before it replies) -
    ///  this function (as all other actor_handle generated functions) are cancel-unsafe,
    ///  using anything that will drop future before its compeletion will result in panic.
    async unsafe fn publish(&self, topic: &'static str, payload: &[u8]) -> Result<(), MqttError> {
        self.__publish(topic, unsafe { launder_slice(payload) }).await
    }
}

impl<const S: usize> From<RawMqttHandle> for SizedMqttHandle<S> {
    fn from(raw: RawMqttHandle) -> Self {
        Self { raw }
    }
}

pub struct SizedMqttHandle<const B: usize> {
    raw: RawMqttHandle,
}

impl<const B: usize> SizedMqttHandle<B> {
    pub fn as_raw(&self) -> RawMqttHandle {
        self.raw.clone()
    }

    pub async fn publish<S: Serialize>(&self, topic: &'static str, data: S) -> Result<(), MqttError> {
        let mut buffer = [0u8; B];
        let payload = match serde_json_core::to_slice(&data, &mut buffer) {
            Ok(payload) => payload,
            Err(e) => {
                return Err(MqttError::Encode(e));
            }
        };
        /*  SAFETY: `buffer` outlives the request - it's not touched again until this
        await resolves, satisfying `publish`'s read-before-reply requirement. */
        unsafe { self.raw.publish(topic, launder_slice(&buffer[..payload])).await }
    }
}

pub struct MqttCredentials {
    pub client_id: String<64>,
    pub username: String<64>,
    pub password: String<64>,
}

pub struct MqttResources<Rng: CryptoRngCore + 'static, const NET: usize = 4096, const TCP: usize = 4096, const TLS: usize = 16640> {
    pub network_stack: Stack<'static>,
    pub mqtt_state: &'static mut State<CriticalSectionRawMutex, NET, NET>,
    pub tls_state: &'static mut MqttTlsState<TLS>,
    pub tcp_client: &'static mut MqttTcpClient<TCP>,
    pub tls_config: &'static TlsConfig<'static>,
    pub rng: Rng,
}

// Accept &A instead of A by value
fn leak_in<T, A: Allocator>(value: T, alloc: &'static A) -> &'static mut T {
    Box::leak(Box::new_in(value, alloc))
}

impl<Rng: CryptoRngCore + 'static, const NET: usize, const TCP: usize, const TLS: usize> MqttResources<Rng, NET, TCP, TLS> {
    pub fn leak_in<A: Allocator>(stack: Stack<'static>, rng: Rng, alloc: &'static A) -> Self {
        Self {
            network_stack: stack,
            mqtt_state: leak_in(MqttState::new(), alloc),
            tls_state: leak_in(MqttTlsState::new(), alloc),
            tcp_client: leak_in(MqttTcpClient::new(stack, leak_in(MqttTcpClientState::new(), alloc)), alloc),
            tls_config: leak_in(TlsConfig::default().enable_rsa_signatures(), alloc),
            rng,
        }
    }
}

pub struct MqttModule<Rng: CryptoRngCore + 'static, const S: usize, const NET: usize = 4096, const TCP: usize = 4096, const TLS: usize = 16640> {
    subscribers: [(&'static str, &'static dyn ErasedHandle); S],
    network_stack: Stack<'static>,
    mqtt_stack: MqttStack<'static, CriticalSectionRawMutex>,
    client: MqttClient<'static, CriticalSectionRawMutex>,
    transport: MqttTlsTransport<Rng, TCP, TLS>,
}
impl<Rng: CryptoRngCore, const S: usize, const NET: usize, const TCP: usize, const TLS: usize> Actor for MqttModule<Rng, S, NET, TCP, TLS> {
    type Handle = RawMqttHandle;

    async fn act(&mut self, mut inbox: ivy_types::actor::Inbox<<Self::Handle as ivy_types::actor::ActorHandle>::Cmd>) -> ! {
        tracing::debug!(tag = "mqtt", "started acting");
        let mqtt_stack_task = Self::run_stack_task(&mut self.mqtt_stack, &mut self.transport, self.network_stack.clone());
        tracing::debug!(tag = "mqtt", "mqtt task was created");
        let topics: [SubscribeTopic<'static>; S] = core::array::from_fn(|i| {
            let (name, _handle) = self.subscribers[i];
            name.into()
        });

        let client_task = Self::run_client_task(&self.subscribers, &self.client, &topics, &mut inbox);
        tracing::debug!(tag = "mqtt", "client task was created");
        join(client_task, mqtt_stack_task).await.1
    }
}

impl<Rng: CryptoRngCore, const S: usize, const NET: usize, const TCP: usize, const TLS: usize> MqttModule<Rng, S, NET, TCP, TLS> {
    pub fn new(res: MqttResources<Rng, NET, TCP, TLS>, creds: MqttCredentials, subscribers: [(&'static str, &'static dyn ErasedHandle); S]) -> Self {
        tracing::info!(tag = "mqtt", "creating mqtt module");
        static CREDS: StaticCell<MqttCredentials> = StaticCell::new();
        let creds = CREDS.init(creds);
        let configuration = Config::builder()
            .client_id(creds.client_id.clone())
            .connect_timeout(Duration::from_secs(20))
            .backoff_algo(|attempt| {
                let max_attempts = 7;
                if attempt >= max_attempts {
                    return None;
                }
                let base_time_ms: u32 = 500;
                let backoff = base_time_ms.saturating_mul(u32::pow(2, attempt as u32));

                Some(Duration::from_millis(backoff.into()))
            })
            .password(creds.password.as_bytes())
            .username(creds.username.as_str())
            .build();

        let (mqtt_stack, client) = mqttrust::new(res.mqtt_state, configuration);
        let provider = UnsecureProvider::new::<Aes128GcmSha256>(res.rng);
        let transport = TlsNalTransport::new(res.tcp_client, IpBroker::new(Ipv4Addr::new(217, 195, 48, 206), 1883), res.tls_state, res.tls_config, provider);

        tracing::info!(tag = "mqtt", "created mqtt module");
        Self {
            subscribers,
            mqtt_stack,
            client,
            transport,
            network_stack: res.network_stack,
        }
    }

    /// Runs mqtt stack: handles wifi disconnects.
    async fn run_stack_task(mqtt_stack: &mut MqttStack<'static, CriticalSectionRawMutex>, transport: &mut MqttTlsTransport<Rng, TCP, TLS>, network_stack: Stack<'static>) -> ! {
        loop {
            network_stack.wait_config_up().await;
            mqtt_stack.run(transport).await;
            Timer::after(Duration::from_millis(4000)).await;
            // All following is done internally in run of mqtt_stack, but just to make sure
            mqtt_stack.disconnect(transport).await.ok();
            mqtt_stack.reset().await;
            tracing::warn!(tag = "mqtt", "connection lost, reconnecting...");
        }
    }

    /// Runs the main mqtt client task loop: waits for connection, races background workers
    /// against disconnect detection, then drains the inbox until reconnected.
    async fn run_client_task(
        subscribers: &[(&'static str, &'static dyn ErasedHandle); S],
        client: &MqttClient<'static, CriticalSectionRawMutex>,
        topics: &[SubscribeTopic<'static>; S],
        inbox: &mut ivy_types::actor::Inbox<<RawMqttHandle as ivy_types::actor::ActorHandle>::Cmd>,
    ) -> ! {
        loop {
            // don't spin the workers up until we're actually connected
            client.wait_connected().await;
            tracing::debug!(tag = "mqtt", "connected, starting worker tasks");
            // Neither of those tasks, should ever return. Only point of failure is mqtt_client itself, if connection lost all of tasks would be cancelled anyways.
            let inbox_task = Self::handle_inbox_task(client, inbox);
            let sub_task = Self::handle_subscriptions(subscribers, client, topics);
            let disconnect_watch = Self::wait_for_disconnect(client);

            // Neither of those futures, besides disconnect_watch ever returns.
            select3(inbox_task, sub_task, disconnect_watch).await;

            tracing::warn!(tag = "mqtt", "connection lost, draining inbox until reconnect");
            // Drain inbox till disconnected
            select(Self::drain_inbox_task(inbox), client.wait_connected()).await;
        }
    }
    /// Task that waits for client disconnect
    async fn wait_for_disconnect(client: &MqttClient<'static, CriticalSectionRawMutex>) {
        loop {
            if !client.wait_connection_change().await {
                return;
            }
        }
    }
    /// Drains inbox, so that it won't overflow with uncompleted publish requests.
    async fn drain_inbox_task(inbox: &mut ivy_types::actor::Inbox<<RawMqttHandle as ivy_types::actor::ActorHandle>::Cmd>) -> ! {
        loop {
            match inbox.next().await {
                MqttHandleCommand::Publish(c, _, _) => {
                    c.ack_err(MqttError::Disconnected).await;
                }
            }
        }
    }

    /// Handles inbox commands
    /// NOTE: logging here is not permited, as it would cause loop issues for logs
    async fn handle_inbox_task(client: &MqttClient<'static, CriticalSectionRawMutex>, inbox: &mut ivy_types::actor::Inbox<<RawMqttHandle as ivy_types::actor::ActorHandle>::Cmd>) {
        loop {
            match inbox.next().await {
                MqttHandleCommand::Publish(c, topic, payload) => {
                    let publish_pkt = Publish::builder().topic_name(&topic).payload(payload).qos(mqttrust::QoS::AtMostOnce).build();

                    match client.publish(publish_pkt).await {
                        Ok(_) => c.ack().await,
                        Err(e) => {
                            c.ack_err(MqttError::MqttClient(e)).await;
                        }
                    }
                }
            }
        }
    }
    /// Manages subscriptions
    async fn handle_subscriptions(subscribers: &[(&'static str, &'static dyn ErasedHandle); S], client: &MqttClient<'static, CriticalSectionRawMutex>, topics: &[SubscribeTopic<'static>; S]) -> ! {
        loop {
            tracing::debug!(tag = "mqtt", "subscribing to topics");
            let mut back_off_ms = 2000;
            let mut subscription = loop {
                let sub_pkt = Subscribe::builder().topics(topics).build();

                match client.subscribe::<S>(sub_pkt).await {
                    Ok(sub) => {
                        tracing::debug!(tag = "mqtt", "successfully subscribed to topics");
                        break sub;
                    }
                    Err(e) => {
                        tracing::error!(tag = "mqtt", "subscribe failed: {:?}. retrying in {}ms...", e, back_off_ms);
                        Timer::after(Duration::from_millis(back_off_ms)).await;
                        back_off_ms = (back_off_ms * 3 / 2).min(20000);
                    }
                }
            };
            loop {
                match subscription.next_message().await {
                    Some(msg) => {
                        let handle = subscribers.iter().find(|(name, _)| *name == msg.topic_name());
                        let Some(handle) = handle else {
                            tracing::warn!(tag = "mqtt", "no handle found for topic {}", msg.topic_name());
                            continue;
                        };
                        let Err(e) = handle.1.dispatch(&msg.payload()) else {
                            tracing::debug!(tag = "mqtt", "dispatched message for topic {}", msg.topic_name());
                            continue;
                        };
                        tracing::error!(tag = "mqtt", "failed to dispatch error: {}", e);
                    }
                    None => tracing::error!(tag = "mqtt", "received none"),
                }
            }
        }
    }
}
/// Macro to pre-declares subscriptions, that will be managed by mqtt durning app lifetime.
#[macro_export]
macro_rules! declare_subcriptions {
    ( $( $name:ident => $topic:literal : $payload:ty ),+ $(,)? ) => {
        {
            $crate::paste::paste! {
                $(
                    static [<$name:upper _SIGNAL>]: ::embassy_sync::signal::Signal<
                        ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                        $payload,
                    > = ::embassy_sync::signal::Signal::new();
                )+
                pub struct SubscriberReg {
                    $( pub $name: $crate::mqtt::TypedHandle<$payload>, )+
                }
                impl SubscriberReg {
                    fn new() -> Self {
                        Self {
                            $( $name: $crate::mqtt::TypedHandle::new(&[<$name:upper _SIGNAL>]), )+
                        }
                    }
                }
            }
            // everything below is OUTSIDE paste!, so $crate:: stays intact
            static SUBSCRIBER_REG: ::static_cell::StaticCell<SubscriberReg> =
                ::static_cell::StaticCell::new();
            let reg: &'static SubscriberReg = SUBSCRIBER_REG.init(SubscriberReg::new());
            let handles: [(&'static str, &'static dyn $crate::mqtt::ErasedHandle); $crate::count!($($name)+)] = [
                $( ($topic, &reg.$name as &'static dyn $crate::mqtt::ErasedHandle), )+
            ];
            let subs = $crate::paste::paste! {
                ( $( $crate::mqtt::Subscription::new(&[<$name:upper _SIGNAL>]), )+ )
            };
            (handles, subs)
        }
    };
}
#[macro_export]
macro_rules! count {
    () => { 0 };
    ($_head:ident $($tail:ident)*) => { 1 + count!($($tail)*) };
}

const DEFAULT_LOG_OVERHEAD: usize = 100;

pub struct MqttLogger {
    log_topic: &'static str,
    handle: RawMqttHandle,
}

impl MqttLogger {
    pub fn new(log_topic: &'static str, handle: RawMqttHandle) -> Self {
        Self { log_topic, handle }
    }
}

impl LogConsumer for MqttLogger {
    async fn consume_logs(&mut self, log_sink: LogSink) -> ! {
        let mut work_buf = [0u8; LOG_SIZE + TAG_SIZE + DEFAULT_LOG_OVERHEAD];
        loop {
            let log = log_sink.pop().await;
            let Ok(serialized) = serde_json_core::to_slice(&log, &mut work_buf) else {
                continue;
            };
            /* SAFETY: `work_buf` isn't touched again until this await resolves,
            satisfying `publish`'s read-before-reply requirement. */
            unsafe {
                self.handle.publish(self.log_topic, &work_buf[..serialized]).await.ok();
            }
        }
    }
}
