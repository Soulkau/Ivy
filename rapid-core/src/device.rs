use core::ops::Deref;

use heapless::String;
use serde::{Deserialize, Serialize};
use static_cell::StaticCell;
use talky::{device::DeviceType, id::DeviceID};

use crate::storage::{SequantialStorageModule, StorageKey};

#[derive(Serialize, Deserialize)]
pub struct PersistentDeviceMeta {
    pub device_id: DeviceID,
    pub name: String<16>, //DeviceType::dervice_generic_name(ShortID)
}

pub struct DeviceMetadata {
    pub device_type: DeviceType,
    pub persistent: PersistentDeviceMeta,
}

impl Deref for DeviceMetadata {
    type Target = PersistentDeviceMeta;

    fn deref(&self) -> &Self::Target {
        &self.persistent
    }
}

impl DeviceMetadata {
    pub fn new(storage: SequantialStorageModule, device_type: DeviceType) -> &'static Self {
        static META: StaticCell<DeviceMetadata> = StaticCell::new();
        let persistent: PersistentDeviceMeta =
            storage.get(StorageKey::metadata_key()).expect("Device");
        let data = META.init_with(move || DeviceMetadata {
            persistent,
            device_type,
        });
        data
    }
}
