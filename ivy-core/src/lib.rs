#![no_std]
#![feature(generic_const_exprs)]
#![feature(unsafe_cell_access)]
#![allow(incomplete_features)]

use core::ops::Deref;

use embedded_storage::nor_flash::NorFlash;
use embedded_tls::CryptoRngCore;
use talky::device::DeviceActionHandler;

use crate::{bluetooth::BluetoothHandle, device::DeviceMetadata, mqtt::MqttModule, storage::StorageModule, wifi::WifiModule};
pub mod bluetooth;
pub mod connection;
pub mod device;
pub mod logger;
pub mod mqtt;
pub mod storage;
pub mod wifi;

pub struct Ivy<A, F, Trng>
where
    F: NorFlash + 'static,
    A: DeviceActionHandler,
    Trng: CryptoRngCore + 'static,
{
    bluetooth: BluetoothHandle,
    wifi: WifiModule, //Currently, just assume kind user have turned on wifi.
    mqtt: MqttModule<Trng>,
    storage: StorageModule<F>,
    metadata: &'static DeviceMetadata,
    device_app: A,
}

impl<A, F, Trng> Deref for Ivy<A, F, Trng>
where
    F: NorFlash,
    A: DeviceActionHandler,
    Trng: CryptoRngCore,
{
    type Target = A;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.device_app
    }
}

impl<A, F, Trng> Ivy<A, F, Trng>
where
    A: DeviceActionHandler,
    F: NorFlash,
    Trng: CryptoRngCore,
{
    fn new(bluetooth: BluetoothHandle, storage: StorageModule<F>, wifi: WifiModule, mqtt: MqttModule<Trng>, metadata: &'static DeviceMetadata, device_app: A) -> Self {
        Self {
            bluetooth,
            storage,
            wifi,
            mqtt,
            metadata,
            device_app,
        }
    }

    fn run(&self) {}
}

#[cfg(test)]
pub mod tests {

    use super::*;

    #[tokio::test]
    async fn ss() {}
}
