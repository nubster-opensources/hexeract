use clap::Args;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::RabbitMqConnectionConfig;

use crate::error::CliError;

/// The transport policy shared by every `hexeract bus` command.
///
/// A broker session carries the broker password in its AMQP handshake, so an
/// `amqp://` URI pointed at anything but loopback publishes that password to
/// every host on the path. The library refuses it by default; these flags are
/// how an operator overrides that refusal from the command line, deliberately
/// and one invocation at a time.
#[derive(Args, Debug, Default)]
pub(crate) struct TransportSecurityArgs {
    /// Permit an unencrypted `amqp://` session to a broker outside loopback.
    ///
    /// The AMQP handshake sends the broker password in cleartext, so anyone
    /// between this machine and the broker can read it, and anyone able to
    /// rewrite the traffic can redirect the messages. Use `amqps://` instead
    /// wherever the broker offers it. This flag exists for a broker that has
    /// no TLS listener yet, and it must be passed on every invocation so that
    /// the choice stays visible in the shell history and in the runbook.
    #[arg(long)]
    insecure_plaintext: bool,
}

impl TransportSecurityArgs {
    /// Open a broker connection under the policy these flags select.
    ///
    /// Warns on stderr whenever the refusal is overridden, so an unencrypted
    /// session always leaves a trace next to the command that opened it.
    ///
    /// # Errors
    ///
    /// Returns [`CliError::Fatal`] when the broker refuses the connection, is
    /// unreachable, or when the URI selects a transport this policy declines.
    pub(crate) async fn connect(&self, conn: &str) -> Result<RabbitMqConnection, CliError> {
        if self.insecure_plaintext {
            eprintln!(
                "Warning: --insecure-plaintext is set: this session is unencrypted and sends the broker password in cleartext."
            );
        }
        RabbitMqConnection::connect_with_config(conn, &self.connection_config())
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))
    }

    /// The library configuration these flags describe.
    fn connection_config(&self) -> RabbitMqConnectionConfig {
        let config = RabbitMqConnectionConfig::default();
        if self.insecure_plaintext {
            config.allow_insecure_plaintext_transport()
        } else {
            config
        }
    }

    /// Whether the caller opted out of the cleartext refusal.
    #[cfg(test)]
    pub(crate) fn is_insecure_plaintext_allowed(&self) -> bool {
        self.insecure_plaintext
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    /// Flattening a shared [`Args`] into a command is easy to get wrong in a
    /// way no compiler catches: a renamed or missing `#[command(flatten)]`
    /// silently drops the flag. This harness pins the parsed surface.
    #[derive(Parser, Debug)]
    struct Harness {
        #[command(flatten)]
        transport: TransportSecurityArgs,
    }

    #[test]
    fn the_cleartext_refusal_holds_unless_the_flag_is_passed() {
        let guarded = Harness::parse_from(["hexeract"]).transport;
        assert!(
            !guarded.is_insecure_plaintext_allowed(),
            "the refusal must be the default"
        );

        let permissive = Harness::parse_from(["hexeract", "--insecure-plaintext"]).transport;
        assert!(
            permissive.is_insecure_plaintext_allowed(),
            "the flag must reach the library configuration"
        );
    }

    /// Without the flag the refusal happens before any socket is opened, so
    /// this test never touches the network. That ordering is the point: a
    /// connect that failed after the handshake would already have published
    /// the password.
    #[tokio::test]
    async fn a_remote_plaintext_broker_is_refused_before_connecting() {
        let guarded = Harness::parse_from(["hexeract"]).transport;

        let error = guarded
            .connect("amqp://user:s3cr3t@broker.example:5672")
            .await
            .expect_err("a remote plaintext broker must be refused");

        let rendered = format!("{error:?}");
        assert!(
            !rendered.contains("s3cr3t"),
            "the refusal must stay credential-free, got {rendered}"
        );
    }
}
