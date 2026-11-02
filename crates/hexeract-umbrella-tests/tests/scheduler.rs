//! Verifies that the `scheduler` facade feature re-exports compile and are
//! reachable through the `hexeract` umbrella crate.

use hexeract::scheduler::{
    InMemoryScheduleStore, ScheduleSink, ScheduledMessage, SchedulerBuilder, SchedulerError,
};

struct NoopSink;

impl ScheduleSink for NoopSink {
    async fn dispatch(&self, _message: &ScheduledMessage) -> Result<(), SchedulerError> {
        Ok(())
    }
}

#[test]
fn scheduler_builder_is_reachable_through_facade() {
    let result = SchedulerBuilder::new(InMemoryScheduleStore::default(), NoopSink).build();
    assert!(result.is_ok(), "default builder config must be valid");
}
