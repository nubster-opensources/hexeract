use std::path::PathBuf;

use clap::Args;
use hexeract_bus::Binding;
use hexeract_bus::BusError;
use hexeract_bus::Exchange;
use hexeract_bus::ExchangeKind;
use hexeract_bus::Queue;
use hexeract_bus::RoutingKey;
use hexeract_bus_rabbitmq::RabbitMqConnection;
use hexeract_bus_rabbitmq::ensure_topology;
use serde::Deserialize;

use crate::error::CliError;

/// CLI arguments for `hexeract bus declare`.
#[derive(Args, Debug)]
pub(crate) struct DeclareArgs {
    /// AMQP connection string (e.g. `amqp://guest:guest@localhost:5672`).
    #[arg(long, env = "HEXERACT_BUS_URL")]
    conn: String,
    /// Path to the TOML topology file.
    #[arg(long)]
    topology: PathBuf,
}

impl DeclareArgs {
    pub(crate) async fn run(self) -> Result<(), CliError> {
        let raw =
            std::fs::read_to_string(&self.topology).map_err(|e| CliError::Fatal(Box::new(e)))?;
        let document: TopologyDocument =
            toml::from_str(&raw).map_err(|e| CliError::Fatal(Box::new(e)))?;
        let (exchanges, queues, bindings) = document
            .into_bus_values()
            .map_err(|e| CliError::Fatal(Box::new(e)))?;

        let connection = RabbitMqConnection::connect(&self.conn)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;
        ensure_topology(&connection, &exchanges, &queues, &bindings)
            .await
            .map_err(|e| CliError::Fatal(Box::new(e)))?;

        println!(
            "declared {} exchange(s), {} queue(s), {} binding(s)",
            exchanges.len(),
            queues.len(),
            bindings.len()
        );
        Ok(())
    }
}

/// TOML schema for `hexeract bus declare --topology FILE`.
#[derive(Debug, Default, Deserialize)]
pub(crate) struct TopologyDocument {
    #[serde(default)]
    pub(crate) exchanges: Vec<ExchangeSpec>,
    #[serde(default)]
    pub(crate) queues: Vec<QueueSpec>,
    #[serde(default)]
    pub(crate) bindings: Vec<BindingSpec>,
}

/// Result of converting a [`TopologyDocument`] into the validated
/// `hexeract-bus` values consumed by `ensure_topology`.
pub(crate) type DeclaredTopology = (Vec<Exchange>, Vec<Queue>, Vec<Binding>);

impl TopologyDocument {
    pub(crate) fn into_bus_values(self) -> Result<DeclaredTopology, BusError> {
        let exchanges = self
            .exchanges
            .into_iter()
            .map(ExchangeSpec::into_bus_value)
            .collect::<Result<Vec<_>, _>>()?;
        let queues = self
            .queues
            .into_iter()
            .map(QueueSpec::into_bus_value)
            .collect::<Result<Vec<_>, _>>()?;
        let bindings = self
            .bindings
            .into_iter()
            .map(BindingSpec::into_bus_value)
            .collect::<Result<Vec<_>, _>>()?;
        Ok((exchanges, queues, bindings))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct ExchangeSpec {
    name: String,
    kind: ExchangeKind,
    #[serde(default = "default_true")]
    durable: bool,
    #[serde(default)]
    auto_delete: bool,
}

impl ExchangeSpec {
    fn into_bus_value(self) -> Result<Exchange, BusError> {
        Ok(Exchange::new(self.name, self.kind)?
            .durable(self.durable)
            .auto_delete(self.auto_delete))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct QueueSpec {
    name: String,
    #[serde(default = "default_true")]
    durable: bool,
    #[serde(default)]
    exclusive: bool,
    #[serde(default)]
    auto_delete: bool,
}

impl QueueSpec {
    fn into_bus_value(self) -> Result<Queue, BusError> {
        Ok(Queue::new(self.name)?
            .durable(self.durable)
            .exclusive(self.exclusive)
            .auto_delete(self.auto_delete))
    }
}

#[derive(Debug, Deserialize)]
pub(crate) struct BindingSpec {
    queue: String,
    exchange: String,
    routing_key: String,
}

impl BindingSpec {
    fn into_bus_value(self) -> Result<Binding, BusError> {
        let routing_key = RoutingKey::new(self.routing_key)?;
        Binding::new(self.queue, self.exchange, routing_key)
    }
}

fn default_true() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_minimal_topology() {
        let toml_doc = r#"
[[exchanges]]
name = "orders.exchange"
kind = "topic"

[[queues]]
name = "orders.received"

[[bindings]]
queue = "orders.received"
exchange = "orders.exchange"
routing_key = "orders.placed"
"#;
        let document: TopologyDocument = toml::from_str(toml_doc).expect("must parse");
        assert_eq!(document.exchanges.len(), 1);
        assert_eq!(document.queues.len(), 1);
        assert_eq!(document.bindings.len(), 1);

        let (exchanges, queues, bindings) =
            document.into_bus_values().expect("conversion must succeed");
        assert_eq!(exchanges[0].name, "orders.exchange");
        assert_eq!(exchanges[0].kind, ExchangeKind::Topic);
        assert!(exchanges[0].durable);
        assert!(!exchanges[0].auto_delete);
        assert_eq!(queues[0].name, "orders.received");
        assert!(queues[0].durable);
        assert_eq!(bindings[0].queue, "orders.received");
        assert_eq!(bindings[0].exchange, "orders.exchange");
        assert_eq!(bindings[0].routing_key.as_str(), "orders.placed");
    }

    #[test]
    fn defaults_to_durable_true_when_field_omitted() {
        let toml_doc = r#"
[[exchanges]]
name = "orders.exchange"
kind = "direct"

[[queues]]
name = "orders.received"
"#;
        let document: TopologyDocument = toml::from_str(toml_doc).expect("must parse");
        let (exchanges, queues, _) = document.into_bus_values().expect("must convert");
        assert!(exchanges[0].durable);
        assert!(queues[0].durable);
    }

    #[test]
    fn explicit_auto_delete_is_propagated() {
        let toml_doc = r#"
[[exchanges]]
name = "orders.exchange"
kind = "fanout"
durable = false
auto_delete = true

[[queues]]
name = "orders.received"
durable = false
auto_delete = true
exclusive = true
"#;
        let document: TopologyDocument = toml::from_str(toml_doc).expect("must parse");
        let (exchanges, queues, _) = document.into_bus_values().expect("must convert");
        assert!(!exchanges[0].durable);
        assert!(exchanges[0].auto_delete);
        assert!(!queues[0].durable);
        assert!(queues[0].auto_delete);
        assert!(queues[0].exclusive);
    }

    #[test]
    fn rejects_invalid_exchange_name() {
        let toml_doc = r#"
[[exchanges]]
name = ""
kind = "topic"
"#;
        let document: TopologyDocument = toml::from_str(toml_doc).expect("must parse");
        let err = document
            .into_bus_values()
            .expect_err("validation must fail");
        assert!(matches!(err, BusError::InvalidTopology { .. }));
    }

    #[test]
    fn empty_document_is_acceptable() {
        let document: TopologyDocument = toml::from_str("").expect("must parse");
        let (exchanges, queues, bindings) = document.into_bus_values().expect("must convert");
        assert!(exchanges.is_empty());
        assert!(queues.is_empty());
        assert!(bindings.is_empty());
    }
}
