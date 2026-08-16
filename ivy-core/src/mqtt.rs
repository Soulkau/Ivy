use core::{marker::PhantomData, net::Ipv4Addr};

use embassy_futures::{
    join::join,
    select::{Either, select},
};
use embassy_net::{
    Stack,
    tcp::client::{TcpClient, TcpClientState},
};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embassy_time::{Duration, Timer};
use embedded_tls::{Aes128GcmSha256, CryptoRng, CryptoRngCore, TlsConfig, UnsecureProvider};
use ivy_macros::actor_handle;
use ivy_types::actor::Actor;
use mqttrust::{
    Config, IpBroker, MqttClient, MqttStack, Publish, State, Subscribe,
    transport::embedded_tls::{TlsNalTransport, TlsState},
};
use serde::Serialize;
use static_cell::StaticCell;

use crate::logger::LOG_CHANNEL;

const MAX_NET_PAYLOAD_SIZE: usize = 4096;

const TCP_BUFFER_SIZE: usize = 4096;

const TLS_BUFFER_SIZE: usize = 16640;

const _: () = assert!(MAX_NET_PAYLOAD_SIZE + 500 <= TLS_BUFFER_SIZE);

#[actor_handle(MqttHandle)]
pub trait MqttHandle<const MAX_MESSAGE_SIZE: usize> {
    async fn __publish(&self, topic: &'static str, payload: [u8; MAX_MESSAGE_SIZE], len: usize);

    async fn subscribe(&self, topic: &'static str);
}

impl<const MAX_MESSAGE_SIZE: usize> MqttHandle<MAX_MESSAGE_SIZE> {
    async fn publish<S: Serialize>(&self, topic: &'static str, data: S) {
        let mut buffer = [0u8; MAX_MESSAGE_SIZE];
        let payload = match serde_json_core::to_slice(&data, &mut buffer) {
            Ok(payload) => payload,
            Err(_) => return,
        };
        self.__publish(topic, buffer, payload).await;
    }
}

pub struct MqttModule<Rng: CryptoRngCore + 'static, const MAX_MESSAGE_SIZE: usize> {
    stack: Stack<'static>,
    trng: Option<Rng>,
}

impl<Rng: CryptoRngCore, const MAX_MESSAGE_SIZE: usize> Actor for MqttModule<Rng, MAX_MESSAGE_SIZE> {
    type Handle = MqttHandle<MAX_MESSAGE_SIZE>;

    async fn act(&mut self, inbox: ivy_types::actor::Inbox<<Self::Handle as ivy_types::actor::ActorHandle>::Cmd>) -> ! {
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

        let client_task = async move {
            let topics = ["mushclim/listen".into()];
            let mut subscription = client.subscribe::<1>(Subscribe::builder().topics(&topics).build()).await.unwrap();

            tracing::info!("[MqttModule] Running MQTT client task");
            let mut work_buf = [0u8; 1024];
            loop {
                let log_sending_task = async {
                    let log = LOG_CHANNEL.receive().await;
                    let serialized = postcard::to_slice(&log, &mut work_buf).unwrap();
                    client
                        .publish(Publish::builder().topic_name("mushclim/log").payload(&*serialized).qos(mqttrust::QoS::AtLeastOnce).build())
                        .await
                        .unwrap();
                };

                match select(subscription.next_message(), log_sending_task).await {
                    Either::First(Some(msg)) => {}
                    Either::Second(_) => {}
                    _ => {}
                }
            }
        };

        join(client_task, mqtt_stack_task).await.0
    }
}

impl<Rng: CryptoRngCore, const MAX_MESSAGE_SIZE: usize> MqttModule<Rng, MAX_MESSAGE_SIZE> {
    pub fn new(stack: Stack<'static>, trng: Rng) -> Self {
        Self { stack, trng: Some(trng) }
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
