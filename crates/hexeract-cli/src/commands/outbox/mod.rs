use clap::Subcommand;

pub mod apply;
pub mod check;
pub mod patch;

/// Actions targeting the outbox storage.
#[derive(Subcommand, Debug)]
pub enum OutboxAction {
    /// Print the canonical outbox schema SQL to stdout, templated with the given table name.
    Patch(patch::PatchArgs),
    /// Apply the canonical outbox schema to a target PostgreSQL database (POC and development only).
    Apply(apply::ApplyArgs),
    /// Validate that the target outbox table exists with the expected columns.
    Check(check::CheckArgs),
}

impl OutboxAction {
    pub async fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Patch(args) => args.run(),
            Self::Apply(args) => args.run().await,
            Self::Check(args) => args.run().await,
        }
    }
}
