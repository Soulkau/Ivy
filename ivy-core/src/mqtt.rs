use core::{marker::PhantomData, net::Ipv4Addr};

use embassy_futures::{
    join::{join, join3},
    select::{Either, select},
};
use embassy_net::{
    Stack,
    tcp::client::{TcpClient, TcpClientState},
};
use embassy_sync::{
    blocking_mutex::raw::CriticalSectionRawMutex,
    channel::{DynamicReceiver, DynamicSender},
};
use embassy_time::{Duration, Timer};
use embedded_tls::{Aes128GcmSha256, CryptoRng, CryptoRngCore, TlsConfig, UnsecureProvider};
use heapless::index_map::FnvIndexMap;
use ivy_macros::actor_handle;
use ivy_types::actor::Actor;
use mqttrust::{
    Config, IpBroker, MqttClient, MqttStack, Publish, State, Subscribe, SubscribeTopic,
    transport::embedded_tls::{TlsNalTransport, TlsState},
};
use serde::{Serialize, de::DeserializeOwned};
use static_cell::StaticCell;

use crate::logger::LOG_CHANNEL;

const MAX_NET_PAYLOAD_SIZE: usize = 4096;

const TCP_BUFFER_SIZE: usize = 4096;

const TLS_BUFFER_SIZE: usize = 16640;

const _: () = assert!(MAX_NET_PAYLOAD_SIZE + 500 <= TLS_BUFFER_SIZE);

#[derive(Debug, thiserror::Error)]
pub enum MqttError {
    #[error("decode error: {0}")]
    Decode(#[from] serde_json_core::de::Error),
    #[error("encode error: {0}")]
    Encode(#[from] serde_json_core::ser::Error),
    #[error("channel full")]
    Full,
}

pub struct Subscription<T: 'static> {
    receiver: DynamicReceiver<'static, T>,
}

impl<T: 'static> Subscription<T> {
    pub fn new(receiver: DynamicReceiver<'static, T>) -> Self {
        Self { receiver }
    }
}

impl<T: 'static> Subscription<T> {
    pub async fn recv(&self) -> T {
        self.receiver.receive().await
    }
}

// the type-erased trait object goes in your registry
pub trait ErasedHandle: Send + Sync {
    fn dispatch(&self, buf: &[u8]) -> Result<(), MqttError>;
}

// the concrete, per-T impl
pub struct TypedHandle<T: 'static> {
    sender: DynamicSender<'static, T>,
}

impl<T: 'static> TypedHandle<T> {
    pub fn new(sender: DynamicSender<'static, T>) -> Self {
        Self { sender }
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

        self.sender.try_send(value).map_err(|_| MqttError::Full)
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
        self.__publish(topic, buffer, payload).await;
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
        while !self.stack.is_config_up() {
            Timer::after(Duration::from_millis(500)).await;
        }
        tracing::info!("[MqttModule] Creating MQTT stack and client");
        let (mqtt_stack, client) = self.setup_mqtt();
        tracing::info!("[MqttModule] MQTT stack and client created");

        tracing::info!("[MqttModule] Creating MQTT stack task");
        let trng = self.trng.take().expect("This should never fail");
        let network_stack = self.stack;

        let mqtt_stack_task = self.run_mqtt_stack(mqtt_stack, network_stack, trng);
        tracing::info!("[MqttModule] MQTT stack task created");

        let topics: [SubscribeTopic<'static>; N] = core::array::from_fn(|i| {
            let (name, _handle) = self.subscribers[i];
            name.into()
        });

        let client_task = async {
            let inbox_task = async {
                loop {
                    match inbox.next().await {
                        MqttHandleCommand::Publish(c, topic, payload, len) => {
                            client
                                .publish(Publish::builder().topic_name(&topic).payload(&payload[..len]).qos(mqttrust::QoS::AtMostOnce).build())
                                .await
                                .unwrap();
                            c.ack().await;
                        }
                        _ => {}
                    }
                }
            };

            let subscription_task = async {
                let mut subscription = client.subscribe::<N>(Subscribe::builder().topics(&topics).build()).await.unwrap();
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
                            tracing::error!("[MqttModule] Failed to dispatch dispatch error: {}", e);
                        }
                        None => tracing::error!("[MqttModule] Received none"),
                    }
                }
            };

            let log_sending_task = async {
                let mut work_buf = [0u8; 1024];
                loop {
                    let log = LOG_CHANNEL.receive().await;
                    let serialized = postcard::to_slice(&log, &mut work_buf).unwrap();
                    client
                        .publish(Publish::builder().topic_name("test/mushclim/log").payload(&*serialized).qos(mqttrust::QoS::AtMostOnce).build())
                        .await
                        .unwrap();
                }
            };

            tracing::info!("[MqttModule] Running MQTT client task");

            join3(inbox_task, log_sending_task, subscription_task).await.0
        };

        join(client_task, mqtt_stack_task).await.1
    }
}

impl<Rng: CryptoRngCore, const S: usize, const N: usize> MqttModule<Rng, S, N> {
    const __ASSERT: () = assert!(S <= MAX_NET_PAYLOAD_SIZE);

    pub fn new(stack: Stack<'static>, trng: Rng, subscribers: [(&'static str, &'static dyn ErasedHandle); N]) -> Self {
        Self { stack, trng: Some(trng), subscribers }
    }

    fn setup_mqtt(&self) -> (MqttStack<'static, CriticalSectionRawMutex>, MqttClient<'static, CriticalSectionRawMutex>) {
        static MQTT_STATE: StaticCell<State<CriticalSectionRawMutex, MAX_NET_PAYLOAD_SIZE, MAX_NET_PAYLOAD_SIZE>> = StaticCell::new();
        let state = MQTT_STATE.init(State::new());
        let configuration = Config::builder()
            .client_id("mushclim-dev".try_into().unwrap())
            .password(option_env!("NATS_PASS").unwrap().as_ref())
            .username(option_env!("NATS_USER").unwrap().as_ref())
            .build();
        tracing::info!("[MqttModule] MQTT configuration built");

        mqttrust::new(state, configuration)
    }

    async fn run_mqtt_stack(&self, mut mqtt_stack: MqttStack<'static, CriticalSectionRawMutex>, network_stack: Stack<'static>, trng: Rng) -> ! {
        let broker = IpBroker::new(Ipv4Addr::new(217, 195, 48, 206), 1883);
        let tls_config = TlsConfig::new().enable_rsa_signatures();
        let tcp_state = TcpClientState::<1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>::new();
        let network = TcpClient::<'_, 1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>::new(network_stack, &tcp_state);
        let tls_state = TlsState::<TLS_BUFFER_SIZE, TLS_BUFFER_SIZE>::new();
        let provider = UnsecureProvider::new::<Aes128GcmSha256>(trng);
        let mut transport = TlsNalTransport::new(&network, broker, &tls_state, &tls_config, provider);
        tracing::info!("[MqttModule] MQTT stack transport created");
        loop {
            mqtt_stack.run(&mut transport).await
        }
    }
}

#[macro_export]
macro_rules! declare_topics {
    ( $( $name:ident => $topic:literal : $payload:ty ),+ $(,)? ) => {
        {
            $crate::paste::paste! {
                $(
                    static [<$name:upper _CHANNEL>]: ::embassy_sync::channel::Channel<
                        ::embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex,
                        $payload,
                        2,
                    > = ::embassy_sync::channel::Channel::new();
                )+

                pub struct SubscriberReg {
                    $( pub $name: $crate::mqtt::TypedHandle<$payload>, )+
                }

                impl SubscriberReg {
                    fn new() -> Self {
                        Self {
                            $( $name: $crate::mqtt::TypedHandle::new([<$name:upper _CHANNEL>].dyn_sender()), )+
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
                ( $( $crate::mqtt::Subscription::new([<$name:upper _CHANNEL>].dyn_receiver()), )+ )
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
