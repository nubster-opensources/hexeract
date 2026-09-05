//! Public surface an application uses to hand its envelope signing and
//! verification material, and its security policy, to the RabbitMQ
//! transport and worker.
//!
//! Neither facade signs or verifies anything itself. [`OutboundEnvelopeSecurity`]
//! and [`InboundEnvelopeSecurity`] only erase the key-source type parameter of
//! [`hexeract_bus::EnvelopeSigner`] and [`hexeract_bus::EnvelopeVerifier`]
//! behind `Arc<dyn ...>`, so an application can share one key source across
//! several transports and workers without that type parameter becoming
//! contagious to every struct that holds one, and carry the configuration to
//! the points that will read it: [`crate::RabbitMqTransport`]'s publish path
//! (issue #444 lot B task 3) and [`crate::RabbitMqWorker`]'s dispatch path
//! (issue #444 lot B task 4).

use std::fmt;
use std::sync::Arc;

use hexeract_bus::Audience;
use hexeract_bus::BusEnvelope;
use hexeract_bus::EnvelopeSecurityConfig;
use hexeract_bus::EnvelopeSecurityError;
use hexeract_bus::EnvelopeSigner;
use hexeract_bus::EnvelopeVerifier;
use hexeract_bus::Issuer;
use hexeract_bus::SecurityHeaders;
use hexeract_bus::SigningContext;
use hexeract_bus::SigningKeySource;
use hexeract_bus::VerificationKeySource;
use hexeract_bus::VerificationPolicy;

/// Signing material and audience a publisher binds to every outbound envelope.
///
/// Carries the configuration [`hexeract_bus::EnvelopeSigner::sign`] needs, and
/// applies it through two crate-private methods, one producing the security
/// headers and one the `user-id` policy: [`crate::RabbitMqTransport`]'s
/// publish path calls both, never [`EnvelopeSigner::sign`] directly, so it
/// never has to know this facade's audience or its `user-id` policy.
pub struct OutboundEnvelopeSecurity {
    issuer: Issuer,
    signer: EnvelopeSigner<Arc<dyn SigningKeySource>>,
    audience: Audience,
    binds_issuer_to_broker_user: bool,
}

impl OutboundEnvelopeSecurity {
    /// Sign every envelope published under this security as `issuer`,
    /// intended for `audience`, using `keys`.
    ///
    /// `keys` is `Arc`-erased so the same key source can back several
    /// transports without making every one of them generic over its
    /// concrete type.
    #[must_use]
    pub fn new(issuer: Issuer, audience: Audience, keys: Arc<dyn SigningKeySource>) -> Self {
        Self {
            signer: EnvelopeSigner::new(issuer.clone(), keys),
            issuer,
            audience,
            binds_issuer_to_broker_user: false,
        }
    }

    /// Publish the issuer as the AMQP `user-id` property, in addition to the
    /// signed `x-hexeract-issuer` header.
    ///
    /// Off by default. RabbitMQ rejects any publish whose `user-id` property
    /// does not match the account that authenticated the connection, so
    /// turning this on requires the deployment to name that broker account
    /// exactly as `issuer`: a mismatch turns every publish into a broker
    /// rejection rather than a signature failure. The signed header alone
    /// already lets a verifier authenticate the issuer end to end, so this
    /// is an additional, broker-enforced binding an operator opts into
    /// deliberately rather than a default every deployment must satisfy.
    #[must_use]
    pub fn bind_issuer_to_broker_user(mut self) -> Self {
        self.binds_issuer_to_broker_user = true;
        self
    }

    /// Sign `envelope` for publication to `destination`.
    ///
    /// Builds the [`SigningContext`] from this facade's own audience, so a
    /// caller only ever supplies the one fact it alone observes: the
    /// destination the envelope is actually being published to. Passing
    /// anything other than that observed destination, such as a value read
    /// back from the envelope, would let the canonical representation bind
    /// to a destination the publish never used.
    ///
    /// # Errors
    ///
    /// Returns whatever [`EnvelopeSigner::sign`] returns.
    pub(crate) fn sign(
        &self,
        envelope: &BusEnvelope,
        destination: &str,
    ) -> Result<SecurityHeaders, EnvelopeSecurityError> {
        let context = SigningContext {
            destination,
            audience: &self.audience,
        };
        self.signer.sign(envelope, &context)
    }

    /// The AMQP `user-id` property to publish alongside a signed envelope.
    ///
    /// `Some(issuer)` only when [`Self::bind_issuer_to_broker_user`] was
    /// called; `None` otherwise. A caller never decides this policy itself,
    /// it only applies whatever this facade returns.
    pub(crate) fn broker_user_id(&self) -> Option<&str> {
        self.binds_issuer_to_broker_user
            .then(|| self.issuer.as_str())
    }
}

/// Renders the issuer, audience and `user-id` binding, never the signer or
/// the key source it wraps.
impl fmt::Debug for OutboundEnvelopeSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OutboundEnvelopeSecurity")
            .field("issuer", &self.issuer)
            .field("audience", &self.audience)
            .field(
                "binds_issuer_to_broker_user",
                &self.binds_issuer_to_broker_user,
            )
            .finish_non_exhaustive()
    }
}

/// Verification material and policy a consumer applies to every inbound
/// envelope.
///
/// Carries the configuration [`hexeract_bus::EnvelopeVerifier::verify`]
/// needs. Wiring it into [`crate::RabbitMqWorker`]'s dispatch path is issue
/// #444 lot B task 4; until then this facade only transports the
/// configuration, except for its crate-private `policy` accessor, which the
/// worker already reads to decide how strictly an unsigned delivery's AMQP
/// properties must be present (see `crate::worker::delivery_to_envelope`).
pub struct InboundEnvelopeSecurity {
    #[expect(
        dead_code,
        reason = "read by issue #444 lot B task 4, which wires EnvelopeVerifier::verify into the dispatch path"
    )]
    verifier: EnvelopeVerifier<Arc<dyn VerificationKeySource>>,
    policy: VerificationPolicy,
}

impl InboundEnvelopeSecurity {
    /// Verify inbound envelopes against `keys`, under `config`.
    ///
    /// `keys` is `Arc`-erased so the same key source can back several
    /// workers without making every one of them generic over its concrete
    /// type.
    ///
    /// `config` is already validated by the time it reaches this
    /// constructor: [`EnvelopeSecurityConfig::builder`]'s `build` refuses a
    /// [`VerificationPolicy::Required`] policy that accepts no audience
    /// before a value exists to pass here. This constructor does not repeat
    /// that check, and does not need to: there is no way to obtain an
    /// `EnvelopeSecurityConfig` that fails it.
    #[must_use]
    pub fn new(keys: Arc<dyn VerificationKeySource>, config: EnvelopeSecurityConfig) -> Self {
        let policy = config.policy();
        Self {
            verifier: EnvelopeVerifier::new(keys, config),
            policy,
        }
    }

    /// The configured verification policy.
    ///
    /// [`EnvelopeVerifier`] takes its [`EnvelopeSecurityConfig`] by value and
    /// does not hand it back, so this facade keeps its own copy: cheap,
    /// since [`VerificationPolicy`] is [`Copy`].
    pub(crate) fn policy(&self) -> VerificationPolicy {
        self.policy
    }
}

/// Renders the configured policy, never the verifier or the key source it
/// wraps.
impl fmt::Debug for InboundEnvelopeSecurity {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("InboundEnvelopeSecurity")
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use ed25519_dalek::SigningKey;
    use hexeract_bus::EnvelopeSecurityError;
    use hexeract_bus::KeyId;
    use hexeract_bus::SigningKeyHandle;
    use hexeract_bus::StaticKeySource;
    use hexeract_bus::VerificationKey;

    use super::*;

    fn issuer() -> Issuer {
        Issuer::new("billing-service").expect("valid issuer")
    }

    fn audience() -> Audience {
        Audience::new("ledger-service").expect("valid audience")
    }

    fn required_config() -> EnvelopeSecurityConfig {
        EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .build()
            .expect("valid configuration")
    }

    #[test]
    fn a_shared_signing_key_source_backs_two_outbound_facades_without_being_cloned() {
        let shared: Arc<dyn SigningKeySource> = Arc::new(StaticKeySource::builder().build());

        let first = OutboundEnvelopeSecurity::new(issuer(), audience(), Arc::clone(&shared));
        let second = OutboundEnvelopeSecurity::new(
            Issuer::new("ledger-service").expect("valid issuer"),
            audience(),
            Arc::clone(&shared),
        );

        // Three strong references (the local `shared` plus the two facades)
        // prove neither constructor cloned the underlying source, only the
        // `Arc` pointing at it.
        assert_eq!(Arc::strong_count(&shared), 3);
        drop(first);
        drop(second);
        assert_eq!(Arc::strong_count(&shared), 1);
    }

    #[test]
    fn a_shared_verification_key_source_backs_two_inbound_facades_without_being_cloned() {
        let shared: Arc<dyn VerificationKeySource> = Arc::new(StaticKeySource::builder().build());

        let first = InboundEnvelopeSecurity::new(Arc::clone(&shared), required_config());
        let second = InboundEnvelopeSecurity::new(Arc::clone(&shared), required_config());

        assert_eq!(Arc::strong_count(&shared), 3);
        drop(first);
        drop(second);
        assert_eq!(Arc::strong_count(&shared), 1);
    }

    #[test]
    fn an_inbound_security_rejects_a_required_policy_without_an_accepted_audience() {
        let keys: Arc<dyn VerificationKeySource> = Arc::new(StaticKeySource::builder().build());

        // The natural calling convention chains the builder straight into
        // the facade constructor. Proving the refusal survives that chain,
        // rather than calling `EnvelopeSecurityConfigBuilder::build` in
        // isolation, is what shows this facade never gets a chance to swallow
        // or re-validate it: an invalid configuration never produces a value
        // `InboundEnvelopeSecurity::new` could accept in the first place.
        let result = EnvelopeSecurityConfig::builder()
            .build()
            .map(|config| InboundEnvelopeSecurity::new(Arc::clone(&keys), config));

        let error = result.expect_err(
            "a required policy with no accepted audience must never reach InboundEnvelopeSecurity::new",
        );
        assert!(matches!(
            error,
            EnvelopeSecurityError::MissingRequiredField {
                field: "accepted_audiences"
            }
        ));
    }

    #[test]
    fn the_outbound_facade_debug_shows_no_key_material() {
        let keys: Arc<dyn SigningKeySource> = Arc::new(
            StaticKeySource::builder()
                .with_signing_key(
                    KeyId::new("2026-09").expect("valid key id"),
                    SigningKeyHandle::from(SigningKey::from_bytes(&[7; 32])),
                )
                .build(),
        );
        let security =
            OutboundEnvelopeSecurity::new(issuer(), audience(), keys).bind_issuer_to_broker_user();

        let rendered = format!("{security:?}");

        // Positive: the public facts a caller needs to reason about this
        // facade without a broker trace are present.
        assert!(
            rendered.contains("billing-service"),
            "rendered as {rendered}"
        );
        assert!(
            rendered.contains("ledger-service"),
            "rendered as {rendered}"
        );
        assert!(
            rendered.contains("binds_issuer_to_broker_user: true"),
            "rendered as {rendered}"
        );

        // Negative: nothing that would unfold the signer, the key source it
        // wraps, or any key material, is present. A test asserting only the
        // positives above would also pass for a `Debug` that additionally
        // dumped the signing key, so this half is load-bearing.
        assert!(
            !rendered.contains("EnvelopeSigner"),
            "rendered as {rendered}"
        );
        assert!(
            !rendered.contains("StaticKeySource"),
            "rendered as {rendered}"
        );
        assert!(
            !rendered.contains("SigningKeyHandle"),
            "rendered as {rendered}"
        );
    }

    #[test]
    fn the_inbound_facade_debug_shows_no_key_material() {
        let keys: Arc<dyn VerificationKeySource> = Arc::new(
            StaticKeySource::builder()
                .with_verification_key(
                    issuer(),
                    KeyId::new("2026-09").expect("valid key id"),
                    VerificationKey::from(SigningKey::from_bytes(&[7; 32]).verifying_key()),
                )
                .build(),
        );
        let security = InboundEnvelopeSecurity::new(keys, required_config());

        let rendered = format!("{security:?}");

        assert!(rendered.contains("Required"), "rendered as {rendered}");
        assert!(
            !rendered.contains("EnvelopeVerifier"),
            "rendered as {rendered}"
        );
        assert!(
            !rendered.contains("StaticKeySource"),
            "rendered as {rendered}"
        );
        assert!(!rendered.contains("VerifyingKey"), "rendered as {rendered}");
    }

    fn signed_envelope() -> BusEnvelope {
        BusEnvelope::restore_from_transport(
            uuid::Uuid::from_u128(1),
            "billing.invoice.issued".to_owned(),
            b"{}".to_vec(),
            uuid::Uuid::from_u128(2),
            None,
            HashMap::new(),
            HashMap::new(),
            std::time::SystemTime::now(),
        )
    }

    fn outbound_security() -> OutboundEnvelopeSecurity {
        let keys: Arc<dyn SigningKeySource> = Arc::new(
            StaticKeySource::builder()
                .with_signing_key(
                    KeyId::new("2026-09").expect("valid key id"),
                    SigningKeyHandle::from(SigningKey::from_bytes(&[7; 32])),
                )
                .build(),
        );
        OutboundEnvelopeSecurity::new(issuer(), audience(), keys)
    }

    fn header_value(headers: &SecurityHeaders, name: &str) -> String {
        headers
            .iter()
            .find(|(header, _)| *header == name)
            .map_or_else(
                || panic!("header {name} is present"),
                |(_, value)| value.to_owned(),
            )
    }

    #[test]
    fn signing_binds_the_observed_destination_and_the_facades_own_audience() {
        let security = outbound_security();

        let headers = security
            .sign(&signed_envelope(), "billing.invoice.issued")
            .expect("signing must succeed");

        assert_eq!(
            header_value(&headers, "x-hexeract-destination"),
            "billing.invoice.issued",
            "the destination header must carry the value the caller passed in, not one read \
             back from the envelope"
        );
        assert_eq!(
            header_value(&headers, "x-hexeract-audience"),
            "ledger-service"
        );
        assert_eq!(
            header_value(&headers, "x-hexeract-issuer"),
            "billing-service"
        );
    }

    #[test]
    fn signing_for_two_destinations_produces_two_different_signatures() {
        let security = outbound_security();
        let envelope = signed_envelope();

        let first = security
            .sign(&envelope, "billing.invoice.issued")
            .expect("signing must succeed");
        let second = security
            .sign(&envelope, "audit.siphon")
            .expect("signing must succeed");

        assert_ne!(
            header_value(&first, "x-hexeract-signature"),
            header_value(&second, "x-hexeract-signature"),
            "a destination passed by the caller must reach the canonical representation, or \
             a message re-routed to another destination would keep a valid signature"
        );
    }

    #[test]
    fn broker_user_id_is_none_unless_the_publisher_binds_it() {
        let security = outbound_security();

        assert_eq!(security.broker_user_id(), None);
    }

    #[test]
    fn broker_user_id_is_the_issuer_once_bound() {
        let security = outbound_security().bind_issuer_to_broker_user();

        assert_eq!(security.broker_user_id(), Some("billing-service"));
    }
}
