use heapless::{String, Vec};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct WifiCredentials {
    pub ssid: String<32>,
    pub password: String<64>,
}

#[derive(Clone)]
pub struct WifiModule {}

impl WifiModule {
    pub fn new() -> Self {
        WifiModule {}
    }
}

trait WifiHandleTrait {
    fn get_network_list(&self) -> u32;
}
