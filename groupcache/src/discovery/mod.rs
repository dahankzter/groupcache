//! Built-in service discovery implementations.
//!
//! Enable via feature flags:
//! - `kubernetes` — discover peers via Kubernetes API server

#[cfg(feature = "kubernetes")]
pub mod kubernetes;
