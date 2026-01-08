use std::error::Error;

/// A factory responsible for creating inbound servers.
///
/// Implementations may create plain HTTP or TLS-enabled servers.
pub trait ServerFactory: Send + Sync {
    fn serve(&self, addr: &str) -> Result<(), Box<dyn Error>>;
}
/// Default server factory using the existing HTTP server implementation.
pub struct DefaultServerFactory;

impl ServerFactory for DefaultServerFactory {
    fn serve(&self, _addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        // NOTE:
        // This will later delegate to the existing scheduler/executor
        // server startup logic.
        //
        // For now, this is a structural placeholder.
        Err("DefaultServerFactory not wired yet".into())
    }
}
/// TLS-enabled server factory (stub).
///
/// This is a placeholder for future TLS support (e.g. using rustls).
pub struct TlsServerFactory;

impl ServerFactory for TlsServerFactory {
    fn serve(&self, _addr: &str) -> Result<(), Box<dyn std::error::Error>> {
        Err("TlsServerFactory is not implemented yet".into())
    }
}
