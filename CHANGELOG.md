# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-11-22

### Added

- `modelBuilder.AddOutbox()` / `AddInbox()` mapping `OutboxMessage` (identity `Sequence`, time-ordered
  `Id`, contract `Type`, JSON `Payload` and `Headers`, partition key, attempts, backoff, lease and
  dead-letter columns, `Version` concurrency token) and `InboxMessage`, with indexes.
- `db.Outbox().Add(message)` and `IHasDomainEvents` collection through `OutboxSaveChangesInterceptor`:
  events are saved in the same transaction as the business change.
- `OutboxTypeRegistry.Map<T>("contract.name.v1", partitionKey)`: explicit allow-list of message types;
  no CLR type names stored and no payload-driven type resolution.
- `OutboxProcessor<TContext>` and the `OutboxRelayService<TContext>` hosted service: lease-based claims
  with optimistic compare-and-swap on any relational provider and `FOR UPDATE SKIP LOCKED` on
  PostgreSQL, exponential backoff with jitter, dead-lettering, per-partition ordering, retention
  cleanup of processed rows, dead letters and inbox records.
- `IOutboxPublisher` with `LoggingPublisher` and `InMemoryPublisher`.
- `TryProcessOnceAsync` for idempotent consumers (handler effects and inbox record in one transaction).
- `Payments.Outbox` meter (published, failed, dead-lettered, lag) and activity source with W3C trace
  context captured on write and restored on publish.
- `samples/Payments.Api` minimal API on SQLite.

[Unreleased]: https://github.com/victorcondackdev/outbox-efcore/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/victorcondackdev/outbox-efcore/releases/tag/v0.1.0
