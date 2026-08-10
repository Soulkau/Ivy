use core::{marker::PhantomData, net::Ipv4Addr};

use embassy_futures::join::join;
use embassy_net::{
    Stack,
    tcp::client::{TcpClient, TcpClientState},
};
use embassy_sync::blocking_mutex::raw::CriticalSectionRawMutex;
use embedded_tls::{Aes128GcmSha256, CryptoRng, CryptoRngCore, TlsConfig, UnsecureProvider};
use ivy_types::Runnable;
use mqttrust::{
    Config, IpBroker, State,
    transport::embedded_tls::{TlsNalTransport, TlsState},
};
use static_cell::StaticCell;

const MAX_NET_PAYLOAD_SIZE: usize = 4096;

const TCP_BUFFER_SIZE: usize = 4096;

const TLS_BUFFER_SIZE: usize = 16640;

const _: () = assert!(MAX_NET_PAYLOAD_SIZE + 500 <= TLS_BUFFER_SIZE);

#[derive(Clone)]
pub struct MqttModule<Rng: CryptoRngCore + 'static> {
    stack: Stack<'static>,
    trng: Rng,
}

impl<Rng: CryptoRngCore> Runnable for MqttModule<Rng> {
    async fn run(self) -> ! {
        static MQTT_STATE: StaticCell<State<CriticalSectionRawMutex, MAX_NET_PAYLOAD_SIZE, MAX_NET_PAYLOAD_SIZE>> = StaticCell::new();
        let state = MQTT_STATE.init(State::new());
        let configuration = Config::builder()
            .client_id("cool-id".try_into().unwrap())
            .password(option_env!("NATS_PASS").unwrap().as_ref())
            .username(option_env!("NATS_USER").unwrap().as_ref())
            .build();
        let (mut mqtt_stack, client) = mqttrust::new(state, configuration);

        let stack = self.stack;
        let mqtt_task = async move {
            let broker = IpBroker::new(Ipv4Addr::new(217, 195, 48, 206), 1883);
            let tls_config = TlsConfig::new().enable_rsa_signatures();
            let tcp_state = TcpClientState::<1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>::new();
            let network = TcpClient::<'_, 1, TCP_BUFFER_SIZE, TCP_BUFFER_SIZE>::new(stack, &tcp_state);
            let tls_state = TlsState::<TLS_BUFFER_SIZE, TLS_BUFFER_SIZE>::new();
            let provider = UnsecureProvider::new::<Aes128GcmSha256>(self.trng);
            let mut transport = TlsNalTransport::new(&network, broker, &tls_state, &tls_config, provider);
            loop {
                mqtt_stack.run(&mut transport).await
            }
        };

        let client_task = async move {
            loop {
                let _ = client;
            }
        };
        join(client_task, mqtt_task).await.0
    }
}

impl<Rng: CryptoRngCore + 'static> MqttModule<Rng> {
    pub fn new(stack: Stack<'static>, trng: Rng) -> Self {
        MqttModule { stack, trng }
    }
}
