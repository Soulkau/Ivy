#![no_std]

use core::{marker::PhantomData, ops::Deref};

use embedded_storage::nor_flash::NorFlash;
use rapid_types::Runnable;
use talky::{
    device::{DeviceActionHandler, DeviceProtocol},
    id::DeviceID,
};

use crate::{
    bluetooth::BluetoothHandle, device::DeviceMetadata, mqtt::MqttModule, storage::StorageModule,
    wifi::WifiModule,
};
pub mod bluetooth;
pub mod device;
mod mqtt;
pub mod storage;
pub mod wifi;

pub struct RapidFramework<A, F>
where
    F: NorFlash + 'static,
    A: DeviceActionHandler + Runnable,
{
    bluetooth: BluetoothHandle,
    wifi: WifiModule,
    mqtt: MqttModule,
    storage: StorageModule<F>,
    metadata: &'static DeviceMetadata,
    device_app: A,
}

impl<A, F> Deref for RapidFramework<A, F>
where
    F: NorFlash,
    A: DeviceActionHandler + Runnable,
{
    type Target = A;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.device_app
    }
}

impl<A, F> RapidFramework<A, F>
where
    A: DeviceActionHandler + Runnable,
    F: NorFlash,
{
    fn new(
        bluetooth: BluetoothHandle,
        storage: StorageModule<F>,
        wifi: WifiModule,
        mqtt: MqttModule,
        metadata: &'static DeviceMetadata,
        device_app: A,
    ) -> Self {
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
