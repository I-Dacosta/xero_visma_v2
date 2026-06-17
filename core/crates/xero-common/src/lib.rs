//! `xero-common` — shared types, errors, and configuration for xero_service_v2.

pub mod config;
pub mod error;
pub mod types;

pub use config::{AppConfig, GcsConfig, SyncConfig, XeroCustomConnectionConfig};
pub use error::{Error, Result};
pub use types::{EntityType, TenantId};
