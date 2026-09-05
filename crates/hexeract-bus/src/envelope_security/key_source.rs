//! Sources of signing and verification key material.
//!
//! The crate defines the contract and ships one in-memory implementation.
//! Wiring a real source (a mounted secret, a vault, a remote key set) is the
//! application's job: the bus core must not depend on any infrastructure
//! detail, and a source that reaches the network belongs behind a cache the
//! application controls.

use std::collections::HashMap;
use std::fmt;

use async_trait::async_trait;
use ed25519_dalek::{SigningKey, VerifyingKey};
use thiserror::Error;

use super::identity::{Issuer, KeyId};

/// Public key material verifying one publisher key.
#[derive(Debug, Clone)]
pub struct VerificationKey(VerifyingKey);

impl VerificationKey {
    pub(crate) fn as_verifying_key(&self) -> &VerifyingKey {
        &self.0
    }
}

impl From<VerifyingKey> for VerificationKey {
    fn from(key: VerifyingKey) -> Self {
        Self(key)
    }
}

/// Handle to the local signing key.
///
/// Its [`fmt::Debug`] implementation prints a placeholder: key material must
/// never reach a log, a trace or a panic message.
pub struct SigningKeyHandle(SigningKey);

impl SigningKeyHandle {
    /// Borrow the underlying Ed25519 signing key.
    pub(crate) fn as_signing_key(&self) -> &SigningKey {
        &self.0
    }
}

impl From<SigningKey> for SigningKeyHandle {
    fn from(key: SigningKey) -> Self {
        Self(key)
    }
}

impl fmt::Debug for SigningKeyHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SigningKeyHandle(redacted)")
    }
}

/// Failure raised by a key source.
///
/// [`fmt::Debug`] is written by hand rather than derived, because the
/// derived implementation would unfold the cause boxed inside
/// [`KeySourceError::Unavailable`], and that cause routinely names a vault
/// endpoint, a key file path or a token. A caller that deliberately wants the
/// cause reaches it through [`std::error::Error::source`], which makes the
/// disclosure a decision rather than a side effect of logging with `?error`.
#[derive(Error)]
#[non_exhaustive]
pub enum KeySourceError {
    /// No key matches the requested issuer and identifier.
    #[error("no key matches the requested identity")]
    UnknownKey,

    /// The requested key is known and explicitly revoked.
    #[error("the requested key is revoked")]
    RevokedKey,

    /// The source could not answer.
    #[error("the key source is unavailable")]
    Unavailable(#[source] Box<dyn std::error::Error + Send + Sync>),

    /// No signing key is configured.
    #[error("no signing key is configured")]
    MissingSigningKey,
}

impl fmt::Debug for KeySourceError {
    /// Renders the variant without ever unfolding a boxed cause.
    ///
    /// The match below is exhaustive on purpose and carries no wildcard arm:
    /// a variant added later stops compiling here until someone decides what
    /// it is allowed to disclose.
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownKey => formatter.write_str("UnknownKey"),
            Self::RevokedKey => formatter.write_str("RevokedKey"),
            Self::Unavailable(_) => formatter.write_str("Unavailable(..)"),
            Self::MissingSigningKey => formatter.write_str("MissingSigningKey"),
        }
    }
}

/// Resolves the public key a signature must be verified against.
#[async_trait]
pub trait VerificationKeySource: Send + Sync {
    /// Resolve the key `key_id` of `issuer`.
    ///
    /// # Errors
    ///
    /// [`KeySourceError::UnknownKey`] when nothing matches,
    /// [`KeySourceError::RevokedKey`] when the key is known and revoked, and
    /// [`KeySourceError::Unavailable`] when the source itself failed.
    async fn verification_key(
        &self,
        issuer: &Issuer,
        key_id: &KeyId,
    ) -> Result<VerificationKey, KeySourceError>;

    /// Reload the key set from its backing store.
    ///
    /// Called at most once per configured interval when an unknown key id is
    /// observed, so a rotation whose keys have not propagated yet resolves for
    /// subsequent envelopes. Implementations that hold a fixed set return
    /// `Ok(())` without doing anything.
    ///
    /// # Errors
    ///
    /// [`KeySourceError::Unavailable`] when the reload failed.
    async fn refresh(&self) -> Result<(), KeySourceError>;
}

/// Provides the local key an outbound envelope is signed with.
pub trait SigningKeySource: Send + Sync {
    /// The key identifier and key material to sign with.
    ///
    /// # Errors
    ///
    /// [`KeySourceError::MissingSigningKey`] when none is configured.
    fn current_signing_key(&self) -> Result<(KeyId, &SigningKeyHandle), KeySourceError>;
}

#[derive(Debug)]
enum KeyState {
    Active(VerificationKey),
    Revoked,
}

/// In-memory key set, fixed at construction.
#[derive(Debug, Default)]
pub struct StaticKeySource {
    verification_keys: HashMap<(Issuer, KeyId), KeyState>,
    signing_key: Option<(KeyId, SigningKeyHandle)>,
}

impl StaticKeySource {
    /// Start building a key set.
    #[must_use]
    pub fn builder() -> StaticKeySourceBuilder {
        StaticKeySourceBuilder::default()
    }
}

#[async_trait]
impl VerificationKeySource for StaticKeySource {
    async fn verification_key(
        &self,
        issuer: &Issuer,
        key_id: &KeyId,
    ) -> Result<VerificationKey, KeySourceError> {
        match self
            .verification_keys
            .get(&(issuer.clone(), key_id.clone()))
        {
            Some(KeyState::Active(key)) => Ok(key.clone()),
            Some(KeyState::Revoked) => Err(KeySourceError::RevokedKey),
            None => Err(KeySourceError::UnknownKey),
        }
    }

    async fn refresh(&self) -> Result<(), KeySourceError> {
        Ok(())
    }
}

impl SigningKeySource for StaticKeySource {
    fn current_signing_key(&self) -> Result<(KeyId, &SigningKeyHandle), KeySourceError> {
        self.signing_key
            .as_ref()
            .map(|(key_id, handle)| (key_id.clone(), handle))
            .ok_or(KeySourceError::MissingSigningKey)
    }
}

/// Builder of a [`StaticKeySource`].
#[derive(Debug, Default)]
pub struct StaticKeySourceBuilder {
    verification_keys: HashMap<(Issuer, KeyId), KeyState>,
    signing_key: Option<(KeyId, SigningKeyHandle)>,
}

impl StaticKeySourceBuilder {
    /// Accept `key` as an active key of `issuer`.
    #[must_use]
    pub fn with_verification_key(
        mut self,
        issuer: Issuer,
        key_id: KeyId,
        key: VerificationKey,
    ) -> Self {
        self.verification_keys
            .insert((issuer, key_id), KeyState::Active(key));
        self
    }

    /// Mark a key of `issuer` as revoked, so it is refused distinctly from an
    /// unknown one.
    #[must_use]
    pub fn with_revoked_key(mut self, issuer: Issuer, key_id: KeyId) -> Self {
        self.verification_keys
            .insert((issuer, key_id), KeyState::Revoked);
        self
    }

    /// Sign outbound envelopes with `key`, announced as `key_id`.
    #[must_use]
    pub fn with_signing_key(mut self, key_id: KeyId, key: SigningKeyHandle) -> Self {
        self.signing_key = Some((key_id, key));
        self
    }

    /// Freeze the key set.
    #[must_use]
    pub fn build(self) -> StaticKeySource {
        StaticKeySource {
            verification_keys: self.verification_keys,
            signing_key: self.signing_key,
        }
    }
}

#[cfg(test)]
mod tests {
    use ed25519_dalek::SigningKey;

    use super::*;

    fn signing_key(seed: u8) -> SigningKey {
        SigningKey::from_bytes(&[seed; 32])
    }

    fn source() -> StaticKeySource {
        let issuer = Issuer::new("billing-service").expect("valid issuer");
        StaticKeySource::builder()
            .with_verification_key(
                issuer.clone(),
                KeyId::new("2026-09").expect("valid key id"),
                VerificationKey::from(signing_key(1).verifying_key()),
            )
            .with_revoked_key(issuer, KeyId::new("2026-06").expect("valid key id"))
            .build()
    }

    #[tokio::test]
    async fn an_active_key_resolves() {
        let issuer = Issuer::new("billing-service").expect("valid issuer");
        let key_id = KeyId::new("2026-09").expect("valid key id");

        let key = source().verification_key(&issuer, &key_id).await;

        assert!(key.is_ok());
    }

    #[tokio::test]
    async fn an_absent_key_is_reported_as_unknown() {
        let issuer = Issuer::new("billing-service").expect("valid issuer");
        let key_id = KeyId::new("2027-01").expect("valid key id");

        let error = source()
            .verification_key(&issuer, &key_id)
            .await
            .expect_err("absent key");

        assert!(matches!(error, KeySourceError::UnknownKey));
    }

    #[tokio::test]
    async fn a_revoked_key_is_distinguished_from_an_unknown_one() {
        let issuer = Issuer::new("billing-service").expect("valid issuer");
        let key_id = KeyId::new("2026-06").expect("valid key id");

        let error = source()
            .verification_key(&issuer, &key_id)
            .await
            .expect_err("revoked key");

        assert!(matches!(error, KeySourceError::RevokedKey));
    }

    #[tokio::test]
    async fn a_key_of_another_issuer_does_not_resolve() {
        let issuer = Issuer::new("ledger-service").expect("valid issuer");
        let key_id = KeyId::new("2026-09").expect("valid key id");

        let error = source()
            .verification_key(&issuer, &key_id)
            .await
            .expect_err("wrong issuer");

        assert!(matches!(error, KeySourceError::UnknownKey));
    }

    #[tokio::test]
    async fn two_keys_of_one_issuer_are_accepted_during_a_rotation_overlap() {
        let issuer = Issuer::new("billing-service").expect("valid issuer");
        let previous = KeyId::new("2026-08").expect("valid key id");
        let current = KeyId::new("2026-09").expect("valid key id");
        let source = StaticKeySource::builder()
            .with_verification_key(
                issuer.clone(),
                previous.clone(),
                VerificationKey::from(signing_key(1).verifying_key()),
            )
            .with_verification_key(
                issuer.clone(),
                current.clone(),
                VerificationKey::from(signing_key(2).verifying_key()),
            )
            .build();

        assert!(source.verification_key(&issuer, &previous).await.is_ok());
        assert!(source.verification_key(&issuer, &current).await.is_ok());
    }

    #[test]
    fn the_signing_key_handle_debug_output_carries_no_key_material() {
        let handle = SigningKeyHandle::from(signing_key(3));

        assert_eq!(format!("{handle:?}"), "SigningKeyHandle(redacted)");
    }

    #[test]
    fn the_unavailable_error_debug_hides_its_source() {
        let inner = std::io::Error::other("SUPER_SECRET_VAULT_TOKEN");
        let error = KeySourceError::Unavailable(Box::new(inner));

        let rendered = format!("{error:?}");

        assert!(
            !rendered.contains("SUPER_SECRET_VAULT_TOKEN"),
            "rendered as {rendered}"
        );
    }

    #[test]
    fn a_source_without_a_signing_key_reports_it() {
        let source = StaticKeySource::builder().build();

        let error = source
            .current_signing_key()
            .expect_err("no signing key configured");

        assert!(matches!(error, KeySourceError::MissingSigningKey));
    }

    #[test]
    fn the_configured_signing_key_is_returned_with_its_identifier() {
        let key_id = KeyId::new("2026-09").expect("valid key id");
        let source = StaticKeySource::builder()
            .with_signing_key(key_id.clone(), SigningKeyHandle::from(signing_key(1)))
            .build();

        let (announced, _handle) = source
            .current_signing_key()
            .expect("a signing key is configured");

        assert_eq!(announced, key_id);
    }

    #[test]
    fn the_key_source_debug_output_carries_no_key_material() {
        let source = StaticKeySource::builder()
            .with_signing_key(
                KeyId::new("2026-09").expect("valid key id"),
                SigningKeyHandle::from(signing_key(1)),
            )
            .build();

        let rendered = format!("{source:?}");

        assert!(rendered.contains("redacted"), "rendered as {rendered}");
    }
}
