pub struct GrpcConfig {}

impl GrpcConfig {
    /// The hard limit of maximum message size the client or server can
    /// **receive**.
    pub const MAX_DECODING_SIZE: usize = 16 * 1024 * 1024;
    /// The hard limit of maximum message size the client or server can
    /// **send**.
    pub const MAX_ENCODING_SIZE: usize = 16 * 1024 * 1024;
}
