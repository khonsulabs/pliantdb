/// A provider of encryption for blocks of data.
pub trait Vault: std::fmt::Debug + Send + Sync + 'static {
    /// Encrypts `payload`, returning a new buffer that contains all information
    /// necessary to decrypt it in the future.
    fn encrypt(&self, payload: &[u8]) -> Vec<u8>;
    /// Decrypts a previously encrypted `payload`, returning the decrypted
    /// information.
    fn decrypt(&self, payload: &[u8]) -> Vec<u8>;
}
