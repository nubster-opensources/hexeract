# Spec d'intégration : Nubster Identity et Hexeract Outbox

> **Auteur** : Nubster Identity (premier client réel)
> **Date** : 2026-05-22
> **Statut** : Discussion thread, input pour le design de la feature Outbox d'Hexeract
> **Cible** : MVP Hexeract suffisant pour débloquer Nubster Identity M1.4 (RegisterAccount), M1.7 (Login), M1.9 (ResetPassword)

## 1. Contexte et motivation

Nubster Identity est en cours de développement (milestone M1). Le use case M1.4 (RegisterAccount) doit livrer atomiquement deux effets :

1. **Persistance opérationnelle** : insertion d'un `Subject`, d'une `Person` et d'un `Account` dans la base PostgreSQL opérationnelle (`nubster_identity_db`).
2. **Événement audit** : insertion d'un événement `AccountRegistered` dans la base PostgreSQL d'audit dédiée (`nubster_audit_db`, séparée pour des raisons SOC 2 / ISO 27001 A.12.4).

Ces deux écritures vivent dans **deux bases distinctes** (defense-in-depth). On ne peut donc pas faire une transaction PostgreSQL XA simple. Les options sont :

| Approche | Atomicité | Coût ops | Verdict |
|---|---|---|---|
| Two-phase commit (XA) | ✅ stricte | élevé (driver, monitoring) | écarté |
| Audit best-effort + warn si fail | ❌ gap audit possible | nul | écarté (gap audit = non-conforme SOC 2) |
| **Outbox transactionnel + worker async** | ✅ eventually consistent | modéré | **adopté** |

Le pattern Outbox est donc fondamental pour Nubster Identity et constitue le premier besoin client concret d'Hexeract.

## 2. Use cases Nubster Identity nécessitant l'Outbox

| Use case | Trigger | Event(s) publié(s) | Milestone |
|---|---|---|---|
| `RegisterAccount` | POST /v1/accounts | `AccountRegistered` | **M1.4** (bloquant) |
| `VerifyEmail` | GET /v1/accounts/verify-email/:token | `EmailVerified` | M1.6 |
| `Login` | POST /v1/auth/login | `LoginSucceeded` ou `LoginFailed` | M1.7 |
| `RequestPasswordReset` | POST /v1/auth/reset-request | `PasswordResetRequested` | M1.8 |
| `ResetPassword` | POST /v1/auth/reset | `PasswordChanged` | M1.9 |
| `Logout` | POST /v1/auth/logout | `LogoutSucceeded` | M1.7 |

Tous ces événements doivent être dispatchés vers la base audit dédiée via un `AuditLogWriter` qui calcule une chaîne HMAC-SHA256 pour la tamper resistance (déjà implémenté dans `nubster-identity-adapter-pg-audit`).

## 3. API Hexeract attendue (côté client)

### 3.1 Publication transactionnelle

L'use case doit pouvoir publier un événement **dans la même transaction PostgreSQL** que ses écritures opérationnelles. Idiome attendu :

```rust
// Pseudo-code Nubster Identity (M1.4)
async fn register_account(
    &self,
    cmd: RegisterAccountCommand,
) -> Result<RegisterAccountOutput, ApplicationError> {
    // ... validation policy, hash password ...
    let mut tx = self.pool.begin().await?;

    self.account_repo.insert(&mut tx, &account).await?;
    self.person_repo.insert(&mut tx, &person).await?;
    self.subject_repo.insert(&mut tx, &subject).await?;

    self.outbox.publish_in_tx(
        &mut tx,
        AccountRegistered { subject_id, occurred_at, ip, user_agent },
    ).await?;

    tx.commit().await?;
    Ok(...)
}
```

**Garantie attendue** : si `tx.commit()` réussit, l'événement EST dans l'outbox. Si `tx.commit()` échoue, ni le state ni l'événement n'existent.

### 3.2 Trait Publisher

```rust
pub trait OutboxPublisher: Send + Sync + 'static {
    fn publish_in_tx<E: Event>(
        &self,
        tx: &mut Self::Tx,
        event: E,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send;

    // Variant outside transaction (best-effort, no atomicity guarantee).
    // Pas nécessaire pour Nubster Identity M1 mais utile pour les health checks.
    fn publish(
        &self,
        event: impl Event,
    ) -> impl Future<Output = Result<(), OutboxError>> + Send;
}
```

**Question ouverte côté Hexeract** : comment exposer le type `Tx` de manière agnostique du backend (PostgreSQL, futur MySQL, etc.) ? Options :
- Associated type (`type Tx<'a>;` avec GATs)
- Trait `Transactional` qui le backend implémente
- API spécifique au backend (`hexeract_pg::Publisher`)

Pour Nubster Identity M1, seul **PostgreSQL** est requis. Une API spécifique au backend est acceptable.

### 3.3 Trait Handler

```rust
#[async_trait]  // ou impl Future + Send selon le style adopté
pub trait Handler<E: Event>: Send + Sync + 'static {
    async fn handle(&self, event: E, ctx: &HandlerContext) -> Result<(), HandlerError>;
}
```

Nubster Identity enregistrera un handler par type d'événement, qui appelle `PgAuditLogWriter::write(audit_entry)`.

### 3.4 Worker

```rust
pub struct OutboxWorker {
    // détails internes
}

impl OutboxWorker {
    pub fn builder() -> OutboxWorkerBuilder;
    pub async fn run(self, cancel: CancellationToken) -> Result<(), WorkerError>;
}

pub struct OutboxWorkerBuilder { /* ... */ }
impl OutboxWorkerBuilder {
    pub fn pool(mut self, pool: deadpool_postgres::Pool) -> Self;
    pub fn register_handler<E: Event>(mut self, handler: impl Handler<E>) -> Self;
    pub fn poll_interval(mut self, d: Duration) -> Self;
    pub fn batch_size(mut self, n: usize) -> Self;
    pub fn build(self) -> OutboxWorker;
}
```

Démarrage typique côté `identityd` :

```rust
let worker = OutboxWorker::builder()
    .pool(op_pool.clone())
    .register_handler::<AccountRegistered>(AuditLogWriterHandler::new(audit_writer.clone()))
    .register_handler::<EmailVerified>(...)
    // ... un handler par event type ...
    .poll_interval(Duration::from_millis(100))
    .batch_size(10)
    .build();

let cancel = CancellationToken::new();
let join = tokio::spawn(worker.run(cancel.clone()));

// ... à l'arrêt :
cancel.cancel();
join.await??;
```

## 4. Schema PostgreSQL outbox attendu

Voici le schema que Nubster Identity a déjà écrit (commit M1.4 commit 1). Hexeract peut s'en inspirer ou imposer son propre format :

```sql
CREATE TABLE audit_outbox (
    id           BIGSERIAL PRIMARY KEY,
    event_type   VARCHAR(64) NOT NULL,
    payload      JSONB       NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at TIMESTAMPTZ
);

CREATE INDEX idx_audit_outbox_pending
    ON audit_outbox (id)
    WHERE delivered_at IS NULL;
```

**Champs essentiels** :
- `id BIGSERIAL` : ordre monotone, base de la détection de gap par audit.
- `event_type VARCHAR(64)` : routing vers le bon handler ; correspond au nom du type Rust (e.g. `"account.registered"`).
- `payload JSONB` : sérialisation de l'événement (libre, défini par l'event type).
- `delivered_at TIMESTAMPTZ NULL` : marqueur de livraison ; partial index pour des polls rapides.

**Points discutables côté Hexeract** :
- Convention du nom de table (`audit_outbox` vs `hexeract_outbox` vs configurable)
- Ajout de champs (e.g. `attempts INTEGER`, `last_error TEXT`, `next_retry_at TIMESTAMPTZ`) pour les retries
- Sharding/partitionnement futur

## 5. Garanties attendues

### 5.1 Atomicité publication
**Stricte** : publication dans la même transaction PG que les writes métier. Si commit fails, rien n'est publié.

### 5.2 Dispatch
**At-least-once** : un événement peut être dispatché plusieurs fois (le handler doit être idempotent). C'est plus simple à garantir que exactly-once.

→ Implication côté Nubster Identity : `PgAuditLogWriter::write` doit être idempotent. La chaîne HMAC l'est déjà naturellement (insert audit_log → si already inserted by previous attempt, on aura un doublon dans la chaîne). Solution : utiliser un identifiant déterministe pour l'entry (e.g. `event_id` UUIDv7 du payload) et un `UNIQUE INDEX` sur `audit_log.event_id` pour dédupliquer.

### 5.3 Ordre
**Partial ordering** : les événements d'un même `subject_id` arrivent dans l'ordre. Pour l'audit log, l'ordre global est garanti par le `pg_advisory_xact_lock` côté writer (déjà implémenté). Donc l'ordre des inserts dans l'outbox détermine l'ordre dans audit_log.

→ Implication : le worker doit dispatcher dans l'ordre des `id` outbox (ORDER BY id) au moins pour les events qui partagent un même subject_id.

### 5.4 Retry
**Implicite** : si le handler échoue (erreur transitoire), `delivered_at` reste NULL et le prochain poll re-essaie. Pas besoin d'un mécanisme explicite pour M1.

**Futur** : exponential backoff + DLQ après N échecs. Hors scope MVP.

### 5.5 Concurrence
**Parallélisme** : `SELECT ... FOR UPDATE SKIP LOCKED` permet à plusieurs workers de tourner en parallèle (utile en K8s avec N replicas). Sans coordination explicite, deux workers ne peuvent pas dispatcher le même event.

## 6. Performance attendue

| Metric | Cible | Justification |
|---|---|---|
| Latence publication (publish_in_tx) | < 5 ms p99 | Insert simple en PG, dans une tx existante |
| Latence dispatch (publish → handler called) | < 200 ms p99 | poll_interval=100ms + batch processing |
| Throughput | 100 events/s soutenu | Volume Nubster Identity en pic : ~10 events/s (registrations, logins). 10× marge confortable |
| Workers concurrents | 1..N | Plusieurs replicas K8s, SKIP LOCKED gère la coordination |

## 7. Sécurité

### 7.1 Payload secrets
**Aucun secret en clair dans le payload** (passwords, hashes, tokens). C'est la responsabilité du caller (Nubster Identity), pas d'Hexeract. Documenter le contrat.

### 7.2 Logging Hexeract
- `Debug` de l'événement / payload ne doit pas logger les payloads sensibles. Idéalement : `tracing` avec `level=INFO` ne logge que `event_type` + `id`, pas le payload entier.
- Configurable : `log_payloads: bool` pour le debug local.

### 7.3 Multi-DB
Le worker Nubster Identity lira l'outbox dans `nubster_identity_db` et écrira l'audit dans `nubster_audit_db`. Il utilise donc DEUX pools. Hexeract doit permettre :
- Le pool **source** (où vit l'outbox) est configuré sur l'OutboxWorker
- Le handler dispose des resources qu'il a en interne (e.g. `AuditLogWriter` avec sa propre pool sur `nubster_audit_db`)

C'est-à-dire : Hexeract ne pilote PAS la connexion du handler. Le handler est libre d'utiliser ce qu'il veut (DB, HTTP, broker).

## 8. Hors scope MVP (pour Nubster Identity M1)

Pour ne pas faire exploser le scope Hexeract MVP, les features suivantes ne sont **pas** nécessaires côté Identity M1 :

- **Bus** : pas de RabbitMQ/NATS/Kafka. L'outbox PG suffit pour l'usage in-process (worker dans le même binaire ou un sidecar local).
- **Sagas** : pas de workflow long. Le password reset M1.9 est court (token + 24h TTL) et géré directement par l'use case.
- **Scheduler** : pas de scheduled messages dans M1. Le crypto-shredding RGPD (T+30j) sera ajouté en M3.
- **Request/Reply** : tous les flows synchrones sont en HTTP/gRPC direct, pas via bus.
- **Mediator in-process** : peut-être en M2 pour découpler use case ↔ handlers. Pas critique en M1.

## 9. Roadmap d'intégration

| Étape | Livrable Hexeract | Livrable Nubster Identity |
|---|---|---|
| 1 | **Outbox MVP** : Publisher, Worker, Handler, backend PG | M1.4 RegisterAccount + endpoint POST /v1/accounts |
| 2 | Stabilisation, audit Hexeract API | M1.5 → M1.10 (audit déjà fait, à wirer via Hexeract) |
| 3 | Hexeract v0.1.0 publié | Nubster Identity v0.2.0 (M1 complet) |

L'**Étape 1** est le chemin critique : tant qu'Hexeract n'a pas livré son MVP Outbox, Nubster Identity M1.4 reste sur sa branche `feature/m1.4-register-account` avec le commit 1 seul (foundation repositories). Le commit 2 (intégration Hexeract) attend Hexeract.

## 10. Questions ouvertes pour Hexeract

1. **Schema d'outbox** : Hexeract impose-t-il un schema ou autorise-t-il un schema custom par client ?
2. **Sérialisation payload** : JSON (sérde_json) imposé ou pluggable (bincode, protobuf...) ?
3. **Versioning des events** : un payload `v1` aujourd'hui, comment évoluer en `v2` ? Tag dans event_type ? Champ `schema_version` ?
4. **Macros / Derive** : `#[derive(Event)]` pour les types d'événements ? Quels invariants vérifiés à la compilation ?
5. **API minimum stable** : à quel point l'API `OutboxWorker` / `OutboxPublisher` peut bouger entre v0.0.x ? Nubster Identity peut s'adapter à des breaking changes pendant la phase pre-alpha, mais idéalement on stabilise une "Surface Outbox v0.1" avant d'en dépendre.

## 11. Contact / handoff

L'agent qui prendra ce document en entrée peut me considérer comme le client de référence pour valider les choix d'API outbox. Toute proposition de design (PR, discussion thread) sur Hexeract doit pouvoir cocher les use cases listés en §2 et les garanties listées en §5.

Nubster Identity team
