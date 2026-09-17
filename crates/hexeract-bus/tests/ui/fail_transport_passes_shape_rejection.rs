use hexeract_bus::{BusEnvelope, ReplyRejection, RequestRegistry};
use uuid::Uuid;

fn main() {
    let registry = RequestRegistry::default();
    let envelope = BusEnvelope::restore(
        Uuid::now_v7(),
        "test.reply".to_owned(),
        Vec::new(),
        Uuid::now_v7(),
        None,
        std::collections::HashMap::new(),
        std::time::SystemTime::now(),
    );

    // Only a transport refusal is accepted here: a shape rejection, which
    // only `reply_acceptance::accepts` may produce, must not type-check.
    registry.resolve(envelope, Err(ReplyRejection::MissingVersion));
}
