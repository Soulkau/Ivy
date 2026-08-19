use core::net::Ipv4Addr;

use embassy_futures::{
    join::join,
    select::{select, select4},
};
use embassy_net::{
    Stack,
    tcp::client::{TcpClient, TcpClientState},
};
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, signal::Signal};
use embassy_time::{Duration, Timer};
use embedded_tls::{Aes128GcmSha256, CryptoRngCore, TlsConfig, UnsecureProvider};
use ivy_macros::actor_handle;
use ivy_types::actor::Actor;
use mqttrust::{
    Config, IpBroker, MqttClient, MqttStack, Publish, State, Subscribe, SubscribeTopic,
    transport::{
        Transport,
        embedded_tls::{TlsNalTransport, TlsState},
    },
};
use serde::{Serialize, de::DeserializeOwned};
use static_cell::StaticCell;

use crate::logger::LOG_CHANNEL;

const MAX_NET_PAYLOAD_SIZE: usize = 4096;

const TCP_BUFFER_SIZE: usize = 4096;

const TLS_BUFFER_SIZE: usize = 16640;

const _: () = assert!(MAX_NET_PAYLOAD_SIZE + 500 <= TLS_BUFFER_SIZE);

type MqttTcpClientState = TcpClientState<1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>;
type MqttTcpClient = TcpClient<'static, 1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>;
type MqttTlsState = TlsState<TLS_BUFFER_SIZE, TLS_BUFFER_SIZE>;
type MqttProvider<Rng> = UnsecureProvider<'static, Aes128GcmSha256, Rng>;
type MqttTlsTransport<Rng> = TlsNalTransport<'static, MqttTcpClient, IpBroker, MqttProvider<Rng>, TLS_BUFFER_SIZE, TLS_BUFFER_SIZE>;

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

// the type-erased trait object goes in your registry
pub trait ErasedHandle: Send + Sync {
    fn dispatch(&self, buf: &[u8]) -> Result<(), MqttError>;
}

// the concrete, per-T impl
pub struct TypedHandle<T: 'static> {
    signal: &'static Signal<CriticalSectionRawMutex, T>,
}

impl<T: 'static> TypedHandle<T> {
    pub fn new(signal: &'static Signal<CriticalSectionRawMutex, T>) -> Self {
        Self { signal }
    }
}

// SAFETY: DynamicSender only exposes try_send, which goes through
// the channel's internal critical-section mutex - it's fine to
// call from multiple threads concurrently as long as T: Send.
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

#[actor_handle(MqttHandle)]
pub trait MqttHandle<const MAX_MESSAGE_SIZE: usize> {
    async fn __publish(&self, topic: &'static str, payload: [u8; MAX_MESSAGE_SIZE], len: usize) -> Result<(), MqttError>;
}

impl<const MAX_MESSAGE_SIZE: usize> MqttHandle<MAX_MESSAGE_SIZE> {
    pub async fn publish<S: Serialize>(&self, topic: &'static str, data: S) {
        let mut buffer = [0u8; MAX_MESSAGE_SIZE];
        let payload = match serde_json_core::to_slice(&data, &mut buffer) {
            Ok(payload) => payload,
            Err(_) => {
                tracing::info!("[MqttHandle] failed to serialize data for publish");
                return;
            }
        };
        tracing::info!("[MqttHandle] Publishing");
        self.__publish(topic, buffer, payload).await.ok();
    }
}

pub struct MqttModule<Rng: CryptoRngCore + 'static, const MAX_MESSAGE_SIZE: usize, const N: usize> {
    subscribers: [(&'static str, &'static dyn ErasedHandle); N],
    stack: Stack<'static>,
    trng: Option<Rng>,
}

impl<Rng: CryptoRngCore, const S: usize, const N: usize> Actor for MqttModule<Rng, S, N> {
    type Handle = MqttHandle<S>;

    async fn act(&mut self, mut inbox: ivy_types::actor::Inbox<<Self::Handle as ivy_types::actor::ActorHandle>::Cmd>) -> ! {
        tracing::info!("[MqttModule] Creating MQTT stack and client");
        let (mqtt_stack, mqtt_client) = Self::create_mqtt_stack();
        tracing::info!("[MqttModule] MQTT stack and client created");
        let trng = self.trng.take().expect("Getting trng should never fail");
        let transport = Self::create_transport(self.stack.clone(), trng);
        tracing::info!("[MqttModule] Transport created");
        let mqtt_stack_task = Self::run_stack_task(mqtt_stack, transport, self.stack.clone());
        tracing::info!("[MqttModule] MQTT stack task created");
        let topics: [SubscribeTopic<'static>; N] = core::array::from_fn(|i| {
            let (name, _handle) = self.subscribers[i];
            name.into()
        });

        let client_task = self.run_client_task(&mqtt_client, &topics, &mut inbox);
        tracing::info!("[MqttModule] Client task created");
        join(client_task, mqtt_stack_task).await.1
    }
}

impl<Rng: CryptoRngCore, const S: usize, const N: usize> MqttModule<Rng, S, N> {
    const __ASSERT: () = assert!(S <= MAX_NET_PAYLOAD_SIZE);

    pub fn new(stack: Stack<'static>, trng: Rng, subscribers: [(&'static str, &'static dyn ErasedHandle); N]) -> Self {
        Self { stack, trng: Some(trng), subscribers }
    }

    fn create_mqtt_stack() -> (MqttStack<'static, CriticalSectionRawMutex>, MqttClient<'static, CriticalSectionRawMutex>) {
        static MQTT_STATE: StaticCell<State<CriticalSectionRawMutex, MAX_NET_PAYLOAD_SIZE, MAX_NET_PAYLOAD_SIZE>> = StaticCell::new();
        let state = MQTT_STATE.init(State::new());
        let configuration = Config::builder()
            .client_id("mushclim".try_into().expect("Failed to create client id"))
            .password(option_env!("NATS_PASS").expect("Failed to get NATS_PASS").as_ref())
            .username(option_env!("NATS_USER").expect("Failed to get NATS_USER").as_ref())
            .build();
        tracing::info!("[MqttModule] MQTT configuration built");

        mqttrust::new(state, configuration)
    }

    fn create_transport(network_stack: Stack<'static>, rng: Rng) -> MqttTlsTransport<Rng> {
        static TCP_STATE: StaticCell<MqttTcpClientState> = StaticCell::new();
        static TLS_STATE: StaticCell<MqttTlsState> = StaticCell::new();
        static NETWORK: StaticCell<MqttTcpClient> = StaticCell::new();
        static TLS_CONFIG: StaticCell<TlsConfig> = StaticCell::new();
        let tcp_state = TCP_STATE.init_with(MqttTcpClientState::new);
        let network = NETWORK.init_with(|| MqttTcpClient::new(network_stack, tcp_state));
        let tls_state = TLS_STATE.init_with(MqttTlsState::new);
        let tls_config = TLS_CONFIG.init_with(|| TlsConfig::new().enable_rsa_signatures());

        let broker = IpBroker::new(Ipv4Addr::new(217, 195, 48, 206), 1883);
        let provider = UnsecureProvider::new::<Aes128GcmSha256>(rng);

        TlsNalTransport::new(network, broker, tls_state, tls_config, provider)
    }

    async fn run_stack_task(mut stack: MqttStack<'static, CriticalSectionRawMutex>, mut transport: MqttTlsTransport<Rng>, network_stack: Stack<'static>) -> ! {
        loop {
            network_stack.wait_config_up().await;
            stack.run(&mut transport).await;
            Timer::after(Duration::from_millis(4000)).await;
            stack.disconnect(&mut transport).await.ok(); // just to make sure, although after run returns it should just return an error
            transport.disconnect().ok(); // in case connection ended dirty
            stack.reset().await; // reset before trying to reconnect
            tracing::info!("[MqttModule] Connection lost, reconnecting...");
        }
    }

    async fn run_client_task(
        &self,
        client: &MqttClient<'static, CriticalSectionRawMutex>,
        topics: &[SubscribeTopic<'static>; N],
        inbox: &mut ivy_types::actor::Inbox<<MqttHandle<S> as ivy_types::actor::ActorHandle>::Cmd>,
    ) -> ! {
        loop {
            // don't spin the workers up until we're actually connected
            client.wait_connected().await;
            tracing::info!("[MqttModule] Connected, starting worker tasks");

            let inbox_task = Self::handle_inbox_task(client, inbox);
            let sub_task = self.handle_subscriptions(client, topics);
            let log_task = Self::handle_log_sending(client);
            let disconnect_watch = Self::wait_for_disconnect(client);

            // whichever future resolves first wins the select - the other three
            // just get dropped in place, which cancels them.
            select4(inbox_task, sub_task, log_task, disconnect_watch).await;

            tracing::warn!("[MqttModule] connection lost, draining inbox until reconnect");
            select(Self::drain_inbox_task(inbox), client.wait_connected()).await;
        }
    }

    async fn wait_for_disconnect(client: &MqttClient<'static, CriticalSectionRawMutex>) {
        loop {
            if !client.wait_connection_change().await {
                return;
            }
        }
    }

    async fn drain_inbox_task(inbox: &mut ivy_types::actor::Inbox<<MqttHandle<S> as ivy_types::actor::ActorHandle>::Cmd>) -> ! {
        loop {
            match inbox.next().await {
                MqttHandleCommand::Publish(c, _, _, _) => {
                    c.ack_err(MqttError::Disconnected).await;
                }
                _ => {}
            }
        }
    }

    async fn handle_inbox_task(client: &MqttClient<'static, CriticalSectionRawMutex>, inbox: &mut ivy_types::actor::Inbox<<MqttHandle<S> as ivy_types::actor::ActorHandle>::Cmd>) {
        loop {
            match inbox.next().await {
                MqttHandleCommand::Publish(c, topic, payload, len) => {
                    let publish_pkt = Publish::builder().topic_name(&topic).payload(&payload[..len]).qos(mqttrust::QoS::AtMostOnce).build();

                    match client.publish(publish_pkt).await {
                        Ok(_) => c.ack().await,
                        Err(e) => {
                            tracing::error!("[MqttModule] Failed to publish message to {}: {:?}", topic, e);
                            c.ack_err(MqttError::MqttClient(e)).await;
                        }
                    }
                }
                _ => {}
            }
        }
    }

    async fn handle_subscriptions(&self, client: &MqttClient<'static, CriticalSectionRawMutex>, topics: &[SubscribeTopic<'static>; N]) {
        loop {
            tracing::info!("[MqttModule] Subscribing to topics");
            let mut back_off_ms = 2000;
            let mut subscription = loop {
                let sub_pkt = Subscribe::builder().topics(topics).build();

                match client.subscribe::<N>(sub_pkt).await {
                    Ok(sub) => {
                        tracing::info!("[MqttModule] Successfully subscribed to topics");
                        break sub;
                    }
                    Err(e) => {
                        tracing::error!("[MqttModule] Subscribe failed: {:?}. Retrying in {}ms...", e, back_off_ms);
                        Timer::after(Duration::from_millis(back_off_ms)).await;
                        back_off_ms = (back_off_ms * 3 / 2).min(20000);
                    }
                }
            };
            loop {
                match subscription.next_message().await {
                    Some(msg) => {
                        let handle = self.subscribers.iter().find(|(name, _)| *name == msg.topic_name());
                        let Some(handle) = handle else {
                            tracing::warn!("[MqttModule] No handle found for topic {}", msg.topic_name());
                            continue;
                        };
                        let Err(e) = handle.1.dispatch(&msg.payload()) else {
                            tracing::info!("[MqttModule] Dispatched message for topic {}", msg.topic_name());
                            continue;
                        };
                        tracing::error!("[MqttModule] Failed to dispatch error: {}", e);
                    }
                    None => tracing::error!("[MqttModule] Received none"),
                }
            }
        }
    }

    async fn handle_log_sending(client: &MqttClient<'static, CriticalSectionRawMutex>) {
        let mut work_buf = [0u8; 1024];
        loop {
            let log = LOG_CHANNEL.receive().await;
            let Ok(serialized) = serde_json_core::to_slice(&log, &mut work_buf) else {
                continue;
            };
            client
                .publish(Publish::builder().topic_name("mushclim/log").payload(&work_buf[..serialized]).qos(mqttrust::QoS::AtMostOnce).build())
                .await
                .ok();
        }
    }
}

#[macro_export]
macro_rules! declare_topics {
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
