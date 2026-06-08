# From MediatR (.NET) to Hexeract

[MediatR](https://github.com/jbogard/MediatR) is the de facto in-process mediator for .NET applications. This guide maps its concepts and API onto Hexeract for engineers moving an existing .NET codebase to Rust or shipping a Rust service alongside one.

## Concept map

| MediatR | Hexeract |
| --- | --- |
| `IRequest<TResponse>` (write side) | `Command` with `type Output = TResponse;` |
| `IRequest<TResponse>` (read side) | `Query` with `type Output = TResponse;` |
| `INotification` | `Notification` (requires `Clone`) |
| `IRequestHandler<TRequest, TResponse>` | `CommandHandler<C>` or `QueryHandler<Q>` |
| `INotificationHandler<TNotification>` | `NotificationHandler<N>` |
| `IMediator.Send(request)` | `mediator.send(command)` / `mediator.query(query)` |
| `IMediator.Publish(notification)` | `mediator.publish(notification)` |
| `IPipelineBehavior<TRequest, TResponse>` | `Middleware` (single onion across all channels) |
| Constructor injection via `IServiceProvider` | Construct your handlers manually and pass to `register_*` |
| `services.AddMediatR()` | `MediatorBuilder::new().register_*().build()` |
| Behavior order = registration order | Same: `with_middleware` registers outermost-first |
| Notification fan-out runs everything, swallows errors by default | Hexeract fans out and **aggregates** failures into a single `HexeractError::Dispatch` |

MediatR collapses Command and Query into a single `IRequest<T>` interface. Hexeract keeps them distinct, with separate registries and methods, because the CQRS triad is part of the trait surface. Functionally the two systems express the same thing.

## Command side by side

MediatR (C#):

```csharp
public sealed record CreateUser(string Email) : IRequest<long>;

public sealed class CreateUserHandler : IRequestHandler<CreateUser, long>
{
    private readonly IUserRepository _repo;
    public CreateUserHandler(IUserRepository repo) => _repo = repo;

    public async Task<long> Handle(CreateUser request, CancellationToken ct)
    {
        return await _repo.InsertAsync(request.Email, ct);
    }
}

// In Program.cs
services.AddMediatR(cfg => cfg.RegisterServicesFromAssembly(typeof(Program).Assembly));
services.AddScoped<IUserRepository, UserRepository>();

// Call site
var id = await mediator.Send(new CreateUser("alice@example.com"));
```

Hexeract (Rust):

```rust
use hexeract::core::{Command, CommandHandler, HandlerContext, HexeractError};
use hexeract::mediator::MediatorBuilder;

pub struct CreateUser { pub email: String }
impl Command for CreateUser {
    type Output = u64;
}

pub struct UserRepository;
# impl UserRepository {
#     pub fn new() -> Self { Self }
#     pub async fn insert(&self, _email: &str) -> Result<u64, HexeractError> { Ok(42) }
# }

pub struct CreateUserHandler {
    repo: UserRepository,
}

impl CommandHandler<CreateUser> for CreateUserHandler {
    type Error = HexeractError;
    async fn handle(&self, cmd: CreateUser, _ctx: &HandlerContext) -> Result<u64, HexeractError> {
        self.repo.insert(&cmd.email).await
    }
}

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mediator = MediatorBuilder::new()
    .register_command_handler::<CreateUser, _>(CreateUserHandler { repo: UserRepository::new() })
    .build()?;

let id = mediator.send(CreateUser { email: "alice@example.com".into() }).await?;
# Ok(()) }
```

Key differences:

- **No DI container.** You hold the handler instance; pass it to `register_command_handler` once. Stateful dependencies (the `UserRepository` here) are fields of the handler struct.
- **`CancellationToken` lives on the context.** `HandlerContext` exposes a `tokio_util::sync::CancellationToken`; the dispatch pipeline observes it before each middleware and before the handler, and returns `HexeractError::Cancelled` once it fired. Middlewares can cancel the token, and handlers can poll `ctx.is_cancelled()` in long cooperative sections. If you need a timeout, wire `TimeoutMiddleware` (see [hexeract-middleware reference](../reference/hexeract-middleware.md)).
- **Async by trait.** Rust 2024 + `trait_variant::make(Send)` makes async-in-traits ergonomic without `Task`/`ConfigureAwait` ceremony.

## Pipeline behavior to middleware

MediatR:

```csharp
public sealed class LoggingBehavior<TRequest, TResponse>
    : IPipelineBehavior<TRequest, TResponse>
{
    public async Task<TResponse> Handle(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken ct)
    {
        Console.WriteLine($"--> {typeof(TRequest).Name}");
        var response = await next();
        Console.WriteLine($"<-- {typeof(TRequest).Name}");
        return response;
    }
}

services.AddTransient(typeof(IPipelineBehavior<,>), typeof(LoggingBehavior<,>));
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

# use hexeract::mediator::MediatorBuilder;
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mediator = MediatorBuilder::new()
    .with_middleware(LoggingMiddleware)
    // .register_*_handler(...)
    .build()?;
# Ok(()) }
```

Notice the difference in scoping. MediatR generates a generic pipeline per request type (one `LoggingBehavior<TRequest, TResponse>` per pair); Hexeract has a single onion that runs around every dispatch, with the type name available through `envelope.type_name()` for filtering or branching.

For built-in equivalents of MediatR's `LoggingBehavior` and `ValidationBehavior`, see [`hexeract-middleware`](../reference/hexeract-middleware.md) (`TracingMiddleware`, `TimeoutMiddleware`).

## Notification fan-out

MediatR runs every handler sequentially and, by default, propagates the first exception. You can swap publishers (`ForeachAwaitPublisher`, `TaskWhenAllPublisher`) to change semantics.

Hexeract runs every handler **concurrently** and **always aggregates** failures: every handler runs regardless of its siblings, and the final `HexeractError::PublishFailed` carries each `NotificationFailure { handler, error }` with its typed error and `source` chain. This is closest to MediatR's `TaskWhenAllPublisher`, except failures are never swallowed: a sibling's error never hides another's.

## Auto-discovery

MediatR scans assemblies at startup and registers every `IRequestHandler` it finds.

Hexeract takes a deliberately different stance: handlers are registered explicitly through the fluent builder. The `#[handler]` macro emits a metadata entry to `inventory`, and `MediatorBuilder::verify_handlers()` cross-checks that every annotated handler was also registered. This catches the most common assembly-scan mistake (forgetting to register a new handler) without taking responsibility for handler instantiation, which would require a runtime DI container that Hexeract chooses not to ship.

```rust,ignore
let builder = MediatorBuilder::new()
    .register_command_handler::<CreateUser, _>(CreateUserHandler { repo: UserRepository::new() });

// Returns Err with the list of handlers visible to #[handler] but absent from the builder.
builder.verify_handlers()?;

let mediator = builder.build()?;
```

See [the `#[handler]` macro page](../concepts/handler-macro.md) for the full rationale.

## What to do about FluentValidation, AutoMapper, MediatR.Extensions.*

These are .NET ecosystem packages with no direct equivalent in the Rust ecosystem; they would each be a separate library decision:

- **FluentValidation**: write validation as a regular Rust function called inside the handler, or as a `Middleware` that branches on `envelope.type_name()`. Look at `validator` and `garde` crates.
- **AutoMapper**: prefer explicit `From`/`Into` implementations or `serde`'s deriving. Cross-struct mapping is typically clearer when written by hand.
- **MediatR.Extensions.Microsoft.DependencyInjection**: not needed. Hexeract has no DI container; you write the wiring code yourself in `main`.

## Migration recipe

A pragmatic order for porting an existing MediatR codebase:

1. **Define your messages.** Each `IRequest<T>` becomes a `Command` or `Query`; each `INotification` becomes a `Notification`. Carry over the field shape verbatim.
2. **Port one slice.** Pick a single command-handler-notification triple and port it (plus the storage trait it depends on). Get it green with `cargo test`.
3. **Wire pipeline behaviors as middlewares.** `TracingMiddleware` covers MediatR's `LoggingBehavior` for free.
4. **Annotate handlers with `#[handler]`.** Run `verify_handlers` in a unit test to catch missing registrations early.
5. **Decommission the .NET service.** Run both side by side behind a feature flag, then cut over.

Hexeract makes step 2 the slow one and steps 3 to 5 trivial. The hardest cultural shift is dropping the DI container reflex.
