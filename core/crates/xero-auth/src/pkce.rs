//! PKCE (Proof Key for Code Exchange) helpers — RFC 7636.

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use rand::Rng;
use sha2::{Digest, Sha256};

/// A PKCE verifier + challenge pair.
#[derive(Debug, Clone)]
pub struct PkceChallenge {
    /// The raw verifier — must be kept secret and sent at token exchange.
    pub verifier: String,
    /// S256 challenge — sent in the authorisation URL.
    pub challenge: String,
}

impl PkceChallenge {
    /// Generate a new random PKCE verifier + S256 challenge.
    pub fn generate() -> Self {
        let verifier = random_verifier(64);
        let challenge = s256_challenge(&verifier);
        Self {
            verifier,
            challenge,
        }
    }
}

fn random_verifier(len: usize) -> String {
    let rng = rand::thread_rng();
    rng.sample_iter(rand::distributions::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn s256_challenge(verifier: &str) -> String {
    let hash = Sha256::digest(verifier.as_bytes());
    URL_SAFE_NO_PAD.encode(hash)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn challenge_length_is_valid() {
        let pkce = PkceChallenge::generate();
        assert!(
            pkce.verifier.len() >= 43,
            "verifier must be ≥43 chars per RFC 7636"
        );
        assert!(!pkce.challenge.is_empty());
    }

    #[test]
    fn two_calls_produce_different_values() {
        let a = PkceChallenge::generate();
        let b = PkceChallenge::generate();
        assert_ne!(a.verifier, b.verifier);
    }
}
