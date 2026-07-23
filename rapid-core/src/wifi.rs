use heapless::{String, Vec};
use rapid_macros::actor;
use rapid_types::{Actor, Runnable};

#[derive(Clone)]
pub struct WifiModule {}

impl WifiModule {
    pub fn new() -> Self {
        WifiModule {}
    }
}

impl Runnable for WifiModule {
    async fn run(self) -> ! {
        loop {}
    }
}

trait WifiHandleTrait {
    fn get_network_list(&self) -> u32;
}
