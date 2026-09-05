//! Whether an unauthenticated envelope may reach a handler.

use std::time::Duration;

use super::error::EnvelopeSecurityError;
use super::identity::Audience;

/// Minimum delay between two key-source refreshes.
pub const DEFAULT_KEY_REFRESH_INTERVAL: Duration = Duration::from_secs(30);

/// What a consumer does with an envelope carrying no valid signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub enum VerificationPolicy {
    /// Refuse any envelope that is not authenticated. The default.
    #[default]
    Required,

    /// Accept unauthenticated envelopes.
    ///
    /// Deliberately verbose: it disables the only defense against a forged
    /// message, and mirrors the transport-level opt-out
    /// `allow_insecure_plaintext_transport`. Intended for local development.
    AllowInsecureUnauthenticatedEnvelopes,
}

/// Envelope security settings of one consumer.
#[derive(Debug, Clone)]
pub struct EnvelopeSecurityConfig {
    policy: VerificationPolicy,
    accepted_audiences: Vec<Audience>,
    key_refresh_interval: Duration,
}

impl EnvelopeSecurityConfig {
    /// Start building a configuration.
    #[must_use]
    pub fn builder() -> EnvelopeSecurityConfigBuilder {
        EnvelopeSecurityConfigBuilder::default()
    }

    /// The configured policy.
    #[must_use]
    pub fn policy(&self) -> VerificationPolicy {
        self.policy
    }

    /// The audiences this consumer accepts an envelope for.
    #[must_use]
    pub fn accepted_audiences(&self) -> &[Audience] {
        &self.accepted_audiences
    }

    /// Minimum delay between two key-source refreshes.
    #[must_use]
    pub fn key_refresh_interval(&self) -> Duration {
        self.key_refresh_interval
    }
}

/// Builder of an [`EnvelopeSecurityConfig`].
#[derive(Debug)]
pub struct EnvelopeSecurityConfigBuilder {
    policy: VerificationPolicy,
    accepted_audiences: Vec<Audience>,
    key_refresh_interval: Duration,
}

impl Default for EnvelopeSecurityConfigBuilder {
    fn default() -> Self {
        Self {
            policy: VerificationPolicy::default(),
            accepted_audiences: Vec::new(),
            key_refresh_interval: DEFAULT_KEY_REFRESH_INTERVAL,
        }
    }
}

impl EnvelopeSecurityConfigBuilder {
    /// Set the verification policy.
    #[must_use]
    pub fn with_policy(mut self, policy: VerificationPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Accept envelopes addressed to `audience`.
    #[must_use]
    pub fn with_accepted_audience(mut self, audience: Audience) -> Self {
        self.accepted_audiences.push(audience);
        self
    }

    /// Override the minimum delay between two key-source refreshes.
    ///
    /// This interval bounds how many times a stream of envelopes carrying an
    /// unrecognized key identifier can force the key source to reload, which
    /// otherwise turns into a network call against a vault on every such
    /// envelope. A zero interval removes that bound entirely and is refused
    /// by [`build`](Self::build). A very low but non-zero interval is
    /// accepted, even though it weakens the protection close to nothing.
    #[must_use]
    pub fn with_key_refresh_interval(mut self, interval: Duration) -> Self {
        self.key_refresh_interval = interval;
        self
    }

    /// Freeze the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`EnvelopeSecurityError::MissingRequiredField`] when the policy
    /// requires verification but no audience is accepted: such a consumer
    /// would refuse every envelope, which is a configuration mistake rather
    /// than a security stance.
    ///
    /// Returns [`EnvelopeSecurityError::InvalidConfiguration`] when the
    /// key-refresh interval is zero: it would disable the rate limit that
    /// bounds how often an unrecognized key identifier can force a key-source
    /// reload.
    pub fn build(self) -> Result<EnvelopeSecurityConfig, EnvelopeSecurityError> {
        if self.policy == VerificationPolicy::Required && self.accepted_audiences.is_empty() {
            return Err(EnvelopeSecurityError::MissingRequiredField {
                field: "accepted_audiences",
            });
        }

        if self.key_refresh_interval.is_zero() {
            return Err(EnvelopeSecurityError::InvalidConfiguration {
                field: "key_refresh_interval",
            });
        }

        Ok(EnvelopeSecurityConfig {
            policy: self.policy,
            accepted_audiences: self.accepted_audiences,
            key_refresh_interval: self.key_refresh_interval,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn audience() -> Audience {
        Audience::new("ledger-service").expect("valid audience")
    }

    #[test]
    fn verification_is_required_by_default() {
        assert_eq!(VerificationPolicy::default(), VerificationPolicy::Required);
    }

    #[test]
    fn a_required_configuration_needs_at_least_one_accepted_audience() {
        let error = EnvelopeSecurityConfig::builder()
            .build()
            .expect_err("no audience");

        assert!(matches!(
            error,
            EnvelopeSecurityError::MissingRequiredField {
                field: "accepted_audiences"
            }
        ));
    }

    #[test]
    fn an_opted_out_configuration_needs_no_audience() {
        let config = EnvelopeSecurityConfig::builder()
            .with_policy(VerificationPolicy::AllowInsecureUnauthenticatedEnvelopes)
            .build()
            .expect("opted out");

        assert!(config.accepted_audiences().is_empty());
    }

    #[test]
    fn the_key_refresh_interval_defaults_to_thirty_seconds() {
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .build()
            .expect("valid configuration");

        assert_eq!(config.key_refresh_interval(), DEFAULT_KEY_REFRESH_INTERVAL);
        assert_eq!(DEFAULT_KEY_REFRESH_INTERVAL, Duration::from_secs(30));
    }

    #[test]
    fn every_declared_audience_is_kept() {
        let config = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .with_accepted_audience(Audience::new("audit-service").expect("valid audience"))
            .build()
            .expect("valid configuration");

        assert_eq!(config.accepted_audiences().len(), 2);
    }

    #[test]
    fn a_zero_key_refresh_interval_is_refused() {
        let error = EnvelopeSecurityConfig::builder()
            .with_accepted_audience(audience())
            .with_key_refresh_interval(Duration::ZERO)
            .build()
            .expect_err("a zero interval disables the rate limit");

        assert!(matches!(
            error,
            EnvelopeSecurityError::InvalidConfiguration {
                field: "key_refresh_interval"
            }
        ));
    }
}
