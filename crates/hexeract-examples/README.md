# hexeract-examples

Runnable, self-asserting examples for the Hexeract messaging framework. Each
example consumes the `hexeract` umbrella crate and exits non-zero if its
scenario does not complete, so they double as smoke tests.

Examples that touch Postgres or RabbitMQ start their own container through
`testcontainers`, so a running Docker daemon is required.

| Example | Demonstrates | Run | Needs Docker |
| --- | --- | --- | --- |
| `01_command_handler` | Stateful `CommandHandler` | `cargo run --example 01_command_handler -p hexeract-examples` | no |
| `02_outbox_transactional` | Atomic business write + event via the transactional outbox (Postgres) | `cargo run --example 02_outbox_transactional -p hexeract-examples` | yes |
| `03_bus_pubsub` | Publish/consume over RabbitMQ | `cargo run --example 03_bus_pubsub -p hexeract-examples` | yes |
| `04_bus_mediator` | Bus consumer dispatching to the in-process CQRS mediator | `cargo run --example 04_bus_mediator -p hexeract-examples` | yes |
| `05_orders_to_payments` | End-to-end: transactional outbox -> RabbitMQ -> mediator | `cargo run --example 05_orders_to_payments -p hexeract-examples` | yes |
