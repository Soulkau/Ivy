use core::range::RangeInclusive;

use const_panic::concat_panic;
use embassy_embedded_hal::adapter::BlockingAsync;
use embassy_sync::{blocking_mutex::raw::CriticalSectionRawMutex, mutex::Mutex};
use embedded_storage::nor_flash::NorFlash;
use sequential_storage::{
    cache::NoCache,
    map::{MapConfig, MapStorage},
};
use serde::{Deserialize, Serialize};

pub struct StorageInner<F: NorFlash> {
    storage: MapStorage<u32, BlockingAsync<F>, NoCache>,
    read_buf: [u8; 128],
    write_buf: [u8; 128],
    work_buf: [u8; 256],
}

pub type Inner<F> = Mutex<CriticalSectionRawMutex, StorageInner<F>>;

pub struct StorageModule<F: NorFlash + 'static> {
    pub inner: &'static Inner<F>,
}

impl<F: NorFlash> StorageModule<F> {
    #[doc(hidden)]
    pub fn build(flash: F, map_config: MapConfig<BlockingAsync<F>>) -> Inner<F> {
        Mutex::new(StorageInner {
            storage: MapStorage::new(BlockingAsync::new(flash), map_config, NoCache::new()),
            read_buf: [0u8; 128],
            write_buf: [0u8; 128],
            work_buf: [0u8; 256],
        })
    }

    pub fn from_static(inner: &'static Inner<F>) -> Self {
        Self { inner }
    }

    pub async fn get<D: for<'de> Deserialize<'de>>(&self, key: StorageKey) -> Option<D> {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard; // reborrow to satisfy borrow checker
        let item_data = inner
            .storage
            .fetch_item(&mut inner.read_buf, key.as_ref())
            .await
            .ok()??;

        let data: D = postcard::from_bytes(item_data).ok()?;

        Some(data)
    }

    pub async fn set<S: Serialize>(&self, key: StorageKey, value: &S) {
        let mut guard = self.inner.lock().await;
        let inner = &mut *guard; // reborrow to satisfy borrow checker

        let serialized: &[u8] = postcard::to_slice(value, &mut inner.read_buf).unwrap();
        let _ = inner
            .storage
            .store_item(&mut inner.work_buf, &key.as_ref(), &serialized)
            .await;
    }
}

#[macro_export]
macro_rules! mk_storage {
    ($flash_ty:ty, $flash:expr, $map_config:expr) => {{
        static CELL: static_cell::StaticCell<$crate::storage::Inner<$flash_ty>> =
            static_cell::StaticCell::new();
        let storage_ref = CELL
            .init_with(|| $crate::storage::StorageModule::<$flash_ty>::build($flash, $map_config));
        $crate::storage::StorageModule::from_static(storage_ref)
    }};
}

pub struct StorageKey(pub(crate) u32);

pub const MIN_KEY_VALUE: u32 = 100;

impl StorageKey {
    pub const fn new(key: u32) -> Self {
        if key < MIN_KEY_VALUE {
            concat_panic!("This ket is reserved for system use: ", key);
        }
        Self(key)
    }

    pub(crate) const fn wifi_key() -> Self {
        Self(0)
    }

    pub(crate) const fn metadata_key() -> Self {
        Self(1)
    }
}

impl AsRef<u32> for StorageKey {
    fn as_ref(&self) -> &u32 {
        &self.0
    }
}
