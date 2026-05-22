//! The 6-dimension Rust messaging framework.
//!
//! Hexeract is an opinionated messaging framework for Rust, inspired by
//! Wolverine (.NET). It unifies in-process mediator dispatch, multi-broker
//! transport, transactional outbox, sagas, scheduler and request/reply
//! in a single ergonomic crate.
//!
//! This crate is a placeholder. The full implementation ships in v0.1.0.
//!
//! # Features (coming in v0.1.0)
//!
//! - **Mediator**: typed command dispatch with zero runtime reflection
//! - **Bus**: unified transport for RabbitMQ, NATS, Kafka, SQS
//! - **Outbox**: transactional outbox/inbox pattern over sqlx
//! - **Sagas**: stateful long-running workflow orchestration
//! - **Scheduler**: delayed and cron-scheduled messages
//! - **Request/Reply**: synchronous RPC over async bus via correlation IDs
