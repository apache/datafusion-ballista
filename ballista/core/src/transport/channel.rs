use std::error::Error;
use std::sync::Arc;

/// A factory responsible for creating outbound client channels.
///
/// This abstraction allows different transport implementations
/// (e.g. plain HTTP, TLS) without changing core logic.
pub trait ChannelFactory: Send + Sync {
    fn create_channel(
        &self,
        target: &str,
    ) -> Result<Arc<dyn std::any::Any + Send + Sync>, Box<dyn Error>>;
}
/// Default channel factory using the existing HTTP-based gRPC connection logic.
pub struct DefaultChannelFactory;

impl ChannelFactory for DefaultChannelFactory {
    fn create_channel(
        &self,
        target: &str,
    ) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, Box<dyn std::error::Error>> {
        // NOTE:
        // For now, this is a placeholder that will be wired
        // to existing client connection logic in the next step.
        //
        // The purpose here is to establish the factory structure
        // without changing behavior.
        Err("DefaultChannelFactory not wired yet".into())
    }
}
/// TLS-enabled channel factory (stub).
///
/// This implementation is intentionally left unimplemented.
/// It exists to demonstrate how TLS support can be plugged in
/// without changing core logic.
pub struct TlsChannelFactory;

impl ChannelFactory for TlsChannelFactory {
    fn create_channel(
        &self,
        _target: &str,
    ) -> Result<std::sync::Arc<dyn std::any::Any + Send + Sync>, Box<dyn std::error::Error>> {
        Err("TlsChannelFactory is not implemented yet".into())
    }
}
