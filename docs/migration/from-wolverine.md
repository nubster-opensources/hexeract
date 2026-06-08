# From Wolverine (.NET) to Hexeract

[Wolverine](https://wolverinefx.net/) is a .NET messaging framework that unifies in-process mediator dispatch, message bus transport, transactional outbox and sagas under one API. Hexeract is the closest Rust analogue: same six-dimension scope (Mediator, Bus, Outbox, Sagas, Scheduler, Request/Reply), with v0.3.0 covering Outbox, Bus and Mediator.

This guide is for engineers porting a Wolverine service to Rust or running both side by side.

## Concept map

| Wolverine | Hexeract |
| --- | --- |
| Message handler (`public void Handle(Foo cmd)` discovered by convention) | `CommandHandler<Foo>` trait impl + explicit registration |
| `IMessageBus.SendAsync(cmd)` (await response) | `mediator.send(cmd)` |
| `IMessageBus.InvokeAsync<T>(cmd)` | `mediator.send(cmd)` (return type is `C::Output`) |
| `IMessageBus.PublishAsync(evt)` (fire and forget) | `mediator.publish(evt)` |
| `[Transactional]` attribute auto-wires outbox | Wire `hexeract-outbox` explicitly through `PgOutboxPublisher::publish_in_tx` |
| `IWolverineMiddleware` (pre/post hooks) | `Middleware` trait (single async `execute` wrapping `next.run`) |
| `[ChainPolicy]` (conditional middlewares) | Branch inside `Middleware::execute` on `envelope.type_name()` |
| `services.UseWolverine(opts => { opts.Discovery.IncludeAssembly(...) })` | `MediatorBuilder::new().register_*().build()` |
| Saga state stored by Wolverine | Not in v0.3.0; sagas planned for the milestone after the v0.3.0 release |
| Scheduled messages (`bus.ScheduleAsync(cmd, time)`) | Not in v0.3.0; scheduler planned |

Wolverine's value proposition is convention over configuration: message handlers are discovered by scanning assemblies for matching signatures. Hexeract chooses explicit registration through the fluent builder, with the `#[handler]` macro emitting metadata for `verify_handlers()` to catch the typo-class of bugs.

## Command side by side

Wolverine (C#):

```csharp
public record CreateUser(string Email);

public static class UserHandler
{
    public static async Task<long> Handle(CreateUser cmd, IUserRepository repo)
        => await repo.InsertAsync(cmd.Email);
}

// Program.cs
builder.Host.UseWolverine();
// Auto-discovered; no explicit registration

// Call site
var id = await bus.InvokeAsync<long>(new CreateUser("alice@example.com"));
```

Hexeract:

```rust
use hexeract::core::{Command, CommandHandler, HandlerContext, HexeractError};
use hexeract::mediator::MediatorBuilder;

pub struct CreateUser { pub email: String }
impl Command for CreateUser {
    type Output = u64;
}

# pub struct UserRepository;
# impl UserRepository {
#     pub fn new() -> Self { Self }
#     pub async fn insert(&self, _email: &str) -> Result<u64, HexeractError> { Ok(42) }
# }
pub struct UserHandler {
    repo: UserRepository,
}

impl CommandHandler<CreateUser> for UserHandler {
    type Error = HexeractError;
    async fn handle(&self, cmd: CreateUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        self.repo.insert(&cmd.email).await
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mediator = MediatorBuilder::new()
    .register_command_handler::<CreateUser, _>(UserHandler { repo: UserRepository::new() })
    .build()?;

let id = mediator.send(CreateUser { email: "alice@example.com".into() }).await?;
# Ok(()) }
```

Where Wolverine reaches into your container for `IUserRepository`, Hexeract has you store the repository on the handler struct and construct the handler yourself in `main`. The trade-off is explicit wiring versus convention.

## Middleware

Wolverine:

```csharp
public class LoggingMiddleware
{
    public static void Before(IChain chain) => Console.WriteLine($"--> {chain.MessageType}");
    public static void After(IChain chain) => Console.WriteLine($"<-- {chain.MessageType}");
}

opts.Policies.AddMiddleware(typeof(LoggingMiddleware));
```

Hexeract:

```rust
use hexeract::core::{BoxOutput, HandlerContext, HexeractError, MessageEnvelope, Middleware, Next};

pub struct LoggingMiddleware;

impl Middleware for LoggingMiddleware {
    async fn execute(
        &self,
        envelope: &MessageEnvelope,
        ctx: &HandlerContext,
        next: Next,
    ) -> Result<BoxOutput, HexeractError> {
        println!("--> {}", envelope.type_name());
        let result = next.run(envelope, ctx).await;
        println!("<-- {}", envelope.type_name());
        result
    }
}
```

Two structural differences:

1. **One method, not pre/post.** Hexeract's `execute` is a single async function around `next.run`. Code "before" runs before the await, code "after" runs after. This composes with `async` borrows in a way that pre/post pairs cannot.
2. **No `IChain`.** The `MessageEnvelope` carries the type name, message id and correlation id. The full chain context is not exposed; if you need to introspect downstream handlers, store that decision in your own builder and surface it through your own envelope extension.

For pre-built tracing and timeout middlewares, see [`hexeract-middleware` reference](../reference/hexeract-middleware.md).

## Outbox

Wolverine couples the outbox to the transaction implicitly with `[Transactional]` and persistence sagas. Hexeract decouples them: you call `PgOutboxPublisher::publish_in_tx` inside your business transaction, and a worker drains the table to the bus on its own polling loop.

See [Outbox quick start](../getting-started/outbox-quick-start.md) and [Outbox pattern](../concepts/outbox-pattern.md) for the runtime model.

The standard pattern is:

```rust,ignore
async fn handle(&self, cmd: CreateUser, ctx: &HandlerContext) -> Result<u64, HexeractError> {
    let mut client = self.pool.get().await?;
    let mut tx = client.transaction().await?;

    let id = self.repo.insert_in_tx(&mut tx, &cmd.email).await?;

    self.outbox.publish_in_tx(&mut tx, &UserCreated { id }).await?;

    tx.commit().await?;
    Ok(id)
}
```

The mediator dispatches `CreateUser` to this handler. The handler runs the business transaction *and* enqueues the outgoing `UserCreated` event in the same transaction. A separate worker, started elsewhere in your service, drains the outbox table to the bus.

This split is more verbose than Wolverine's `[Transactional]` attribute, but it makes the failure semantics observable and testable: there is no magic about when the outbox row is written, because you write it.

## What is missing from Hexeract today

If your Wolverine service relies on these, Hexeract has them on the roadmap:

- **Sagas** with persisted state and timeouts. Planned for the milestone after v0.3.0.
- **Scheduled messages** (`bus.ScheduleAsync`). Planned same.
- **Request/Reply over the bus** (correlation-id-based RPC pattern). Planned same.

For everything covered today (Mediator, Bus, Outbox), Hexeract should give you feature parity with a Rust ergonomic surface.

## Cultural shift

The hardest cultural adjustment moving from Wolverine to Hexeract is the loss of convention-based discovery. In Wolverine, dropping a class into the project is enough; in Hexeract, you must also register it. The `#[handler]` macro narrows the gap by emitting metadata, and `verify_handlers()` makes the "forgot to register" mistake fail fast. But the wiring stays in your hands. That is the deliberate trade-off Rust services tend to prefer: less magic, more grep-ability.
