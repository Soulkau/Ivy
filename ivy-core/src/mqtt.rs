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
use ivy_types::actor::Actor;
use mqttrust::{
    Config, IpBroker, Publish, State, Subscribe,
    transport::embedded_tls::{TlsNalTransport, TlsState},
};
use static_cell::StaticCell;

use crate::logger::LOG_CHANNEL;

const MAX_NET_PAYLOAD_SIZE: usize = 4096;

const TCP_BUFFER_SIZE: usize = 4096;

const TLS_BUFFER_SIZE: usize = 16640;

const _: () = assert!(MAX_NET_PAYLOAD_SIZE + 500 <= TLS_BUFFER_SIZE);

pub struct MqttModule<Rng: CryptoRngCore + 'static> {
    stack: Stack<'static>,
    trng: Option<Rng>,
}

impl<Rng: CryptoRngCore> MqttModule<Rng> {
    pub fn new(stack: Stack<'static>, trng: Rng) -> Self {
        Self { stack, trng: Some(trng) }
    }

    pub async fn run(&mut self) -> ! {
        while !self.stack.is_config_up() {
            Timer::after(Duration::from_millis(500)).await;
        }

        tracing::info!("[MqttModule] Running MQTT module");
        static MQTT_STATE: StaticCell<State<CriticalSectionRawMutex, MAX_NET_PAYLOAD_SIZE, MAX_NET_PAYLOAD_SIZE>> = StaticCell::new();
        let state = MQTT_STATE.init(State::new());
        let configuration = Config::builder()
            .client_id("cool-id".try_into().unwrap())
            .password(option_env!("NATS_PASS").unwrap().as_ref())
            .username(option_env!("NATS_USER").unwrap().as_ref())
            .build();
        tracing::info!("[MqttModule] MQTT configuration built");

        let (mut mqtt_stack, client) = mqttrust::new(state, configuration);
        tracing::info!("[MqttModule] MQTT stack created");

        let stack = self.stack;
        tracing::info!("Staring future 1");

        tracing::info!("[MqttModule] Configurating MQTT transport");
        let broker = IpBroker::new(Ipv4Addr::new(217, 195, 48, 206), 1883);
        let tls_config = TlsConfig::new().enable_rsa_signatures();
        let tcp_state = TcpClientState::<1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>::new();
        let network = TcpClient::<'_, 1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>::new(stack, &tcp_state);
        let tls_state = TlsState::<TLS_BUFFER_SIZE, TLS_BUFFER_SIZE>::new();
        let provider = UnsecureProvider::new::<Aes128GcmSha256>(self.trng.take().expect("This should never fail"));
        let mut transport = TlsNalTransport::new(&network, broker, &tls_state, &tls_config, provider);
        tracing::info!("[MqttModule] MQTT transport created");
        let mqtt_task = async move {
            loop {
                mqtt_stack.run(&mut transport).await
            }
        };

        let client_task = async move {
            let topics = ["mushclim/listen".into()];
            let mut subscription = client.subscribe::<1>(Subscribe::builder().topics(&topics).build()).await.unwrap();

            tracing::info!("[MqttModule] Running MQTT client task");
            let mut work_buf = [0u8; 1024];
            loop {
                let log_sending_task = async {
                    let mut work_buf = [0u8; 256];
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

        join(client_task, mqtt_task).await;
        tracing::info!("Starting future 2, we join them duh");

        unreachable!()
    }
}
