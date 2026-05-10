# ADR-0001: Persist Order Book Snapshots to Azure Blob Storage

## Status

Accepted

## Date

2026-05-10

## Context

The project collects raw order book data for several cryptocurrency exchange pairs. The order book is stored locally while the application is running.

If the application or container goes down, the current local order book state may be lost. This would make recovery difficult and may require rebuilding the order book from exchange APIs or historical events, which can be slow, incomplete, or unavailable.

The system needs a recovery mechanism that allows the application to restore a recent order book state after a crash, restart, deployment, or container replacement.

The key requirements are:

- Preserve recent order book state outside the running container.
- Avoid losing all collected order book data after a crash.
- Support large raw order book files.
- Keep the solution reasonably simple and cost-effective.
- Avoid introducing expensive infrastructure unless necessary.
- Allow recovery per exchange and trading pair.

## Decision

The system will save a snapshot of the current order book to Azure Blob Storage every five minutes.

Each snapshot will be copied from local application storage and uploaded to Azure Blob Storage.

The snapshot should include enough metadata to identify:

- Exchange name
- Trading pair
- Timestamp
- Snapshot version
- Application version, if useful
- Data format
- Compression format, if used

Recommended blob path format:

```text
order-books/
  exchange={exchange}/
    pair={base}-{quote}/
      date={yyyy-mm-dd}/
        snapshot-{yyyy-mm-ddTHH-mm-ssZ}.json.gz
```

Example:

```text
order-books/exchange=binance/pair=BTC-USDT/date=2026-05-10/snapshot-2026-05-10T14-25-00Z.json.gz
```

The application should recover by loading the most recent valid snapshot for each exchange and pair.

## Why Azure Blob Storage

Azure Blob Storage is suitable for storing large binary or text-like files, including raw order book snapshots. It is external to the container lifecycle, so data remains available after container restart, redeployment, or failure.

It is also simpler and usually cheaper than maintaining a message broker only for snapshot persistence.

## Considered Options

### Option 1: Store the order book in a local file inside the container

The application could periodically write the current order book state to a file inside the running container.

#### How it would work

The application would serialize the order book and write it to a local file path, for example:

```text
/app/data/order-book/BTC-USDT.json
```

On restart, the application would try to load this file and restore the order book from it.

#### Advantages

- Simple to implement.
- Very fast read and write operations.
- No dependency on external infrastructure.
- Useful for local development and debugging.

#### Disadvantages

- Data can be lost when the container is restarted, recreated, or rescheduled.
- Not reliable in Docker, Kubernetes, or cloud environments unless backed by a persistent volume.
- Does not work well when the application has multiple replicas.
- Does not provide durable recovery if the host node fails.
- Requires additional backup strategy if data must survive infrastructure failure.

#### Assessment

This option is not reliable enough as the main recovery mechanism. It can be used as a temporary local cache, but not as durable storage.

#### Decision

Rejected.

---

### Option 2: Use a message broker such as RabbitMQ or Kafka

The application could publish order book updates, deltas, or snapshots to a message broker. On recovery, the application could consume messages from the broker and rebuild the order book.

#### How it would work

There are two possible models:

1. Publish every order book update to a topic or queue.
2. Publish periodic full snapshots to a topic or queue.

For example, Kafka topics could be organized by exchange and trading pair:

```text
order-book.binance.btc-usdt
order-book.coinbase.eth-usd
```

The application would recover by replaying retained messages.

#### Advantages

- Strong option for event-driven architecture.
- Kafka can support replaying historical events if retention is configured properly.
- Good fit if multiple downstream services need to consume order book updates.
- Can reduce data loss if every update is persisted.
- Useful if the project later moves toward event sourcing.

#### Disadvantages

- More expensive to operate than object storage.
- Higher operational complexity.
- Requires broker provisioning, monitoring, retention configuration, partitioning, scaling, and alerting.
- Large full snapshots are not an ideal payload for most message brokers.
- RabbitMQ is usually better for message delivery than long-term large-data retention.
- Kafka can store events, but using it mainly for large snapshot files is inefficient compared with object storage.
- Recovery logic can become more complex if snapshots and deltas are mixed.

#### Assessment

A message broker is reliable and powerful, but it is too heavy for the current requirement. The project currently needs periodic durable recovery snapshots, not a full event-streaming platform.

A broker may become useful later if the system needs near-zero data loss, multiple consumers, replay of every order book update, or event sourcing.

#### Decision

Rejected for the current use case.

---

### Option 3: Store order book snapshots in Azure Blob Storage

The application could periodically upload full order book snapshots to Azure Blob Storage.

#### How it would work

Every five minutes, the application would:

1. Read the current order book from local storage or memory.
2. Serialize it to a file format such as JSON, MessagePack, Avro, or Parquet.
3. Compress the file, for example with gzip or zstd.
4. Upload the compressed snapshot to Azure Blob Storage.
5. Update a small pointer file, such as `latest.json`, after the upload succeeds.

On startup, the application would:

1. Read `latest.json` for each exchange and trading pair.
2. Download the latest referenced snapshot.
3. Validate the checksum and schema version.
4. Restore the order book from the snapshot.
5. If the latest snapshot is invalid, try an older snapshot.

#### Advantages

- Durable storage outside the container lifecycle.
- Suitable for large raw files.
- Cost-effective compared with running a message broker only for recovery snapshots.
- Simple operational model.
- Works well for periodic snapshots.
- Easy to organize by exchange, trading pair, and date.
- Supports compression and lifecycle retention policies.
- Allows keeping historical snapshots for debugging or audit purposes.
- Can be accessed by other services if needed.

#### Disadvantages

- Recovery point objective depends on the snapshot interval.
- With a five-minute interval, the system can lose up to approximately five minutes of order book state.
- Blob Storage does not provide replay of every order book update by itself.
- The application must handle partial uploads, corrupted files, invalid snapshots, and schema changes.
- Very frequent snapshots can increase storage and transaction costs.
- Recovery may be slower than reading from a local disk because data must be downloaded from remote storage.

#### Assessment

This option satisfies the current reliability requirement while keeping the architecture simple. It provides durable recovery without introducing a complex broker-based architecture.

The five-minute snapshot interval is an acceptable trade-off if the project can tolerate losing up to five minutes of latest order book changes after a crash.

#### Decision

Accepted.

---

### Option 4: Use a persistent volume or managed disk

The application could write the order book to a persistent volume attached to the container, pod, or virtual machine.

#### How it would work

Instead of storing the order book file inside the container filesystem, the application would write it to a mounted persistent volume, for example:

```text
/mnt/order-book-storage/BTC-USDT.json
```

In Kubernetes, this could be implemented with a PersistentVolumeClaim. In Azure, this could be backed by Azure Disk or Azure Files.

#### Advantages

- Faster than remote object storage for frequent local reads and writes.
- Keeps a simple filesystem-based programming model.
- Data can survive container restarts.
- Useful for low-latency local checkpointing.
- Can be combined with Azure Blob Storage as a local cache.

#### Disadvantages

- More infrastructure-specific than object storage.
- Can be tied to a specific node, region, or deployment setup.
- May be harder to share safely across multiple replicas.
- Does not automatically solve backup, retention, or cross-region durability.
- Recovery can be harder if the underlying node, disk, or volume has problems.
- Requires additional operational management in Kubernetes or cloud infrastructure.

#### Assessment

This option is better than writing to the container filesystem, but it is still not ideal as the primary durable recovery mechanism. It may be useful as a local optimization, especially if snapshots are large and frequent.

#### Decision

Rejected as the primary recovery mechanism. Can be reconsidered as a secondary local checkpointing layer.

---

### Option 5: Use a database optimized for time-series or document storage

The application could store order book snapshots or updates in a database, such as PostgreSQL, TimescaleDB, MongoDB, or ClickHouse.

#### How it would work

The application would write either full snapshots or incremental updates into database tables or collections.

A possible relational structure could be:

```text
order_book_snapshots
- id
- exchange
- trading_pair
- created_at
- schema_version
- snapshot_data
- checksum
```

Alternatively, the snapshot data could be stored as compressed binary data, JSONB, or in a columnar format depending on the database.

#### Advantages

- Easier querying compared with raw files in object storage.
- Useful if the project needs analytics over historical order book data.
- Can support indexing by exchange, trading pair, and timestamp.
- Mature backup and replication options are available for many databases.
- Good option if snapshots need to be searched, filtered, or joined with other data.

#### Disadvantages

- More expensive and complex than Blob Storage for storing large raw files.
- Large binary snapshots can put pressure on database storage, memory, indexes, and backups.
- Requires schema design and database maintenance.
- May be inefficient if the only required operation is “save latest snapshot and restore it later.”
- Scaling storage for large raw order book data may become costly.

#### Assessment

A database is useful if the project needs querying and analytics. For simple durable snapshot storage, it is less suitable than Blob Storage.

#### Decision

Rejected for the current recovery use case. Can be considered later for analytics or historical querying.

## Decision Outcome

Azure Blob Storage will be used as the durable storage for periodic order book snapshots.

Snapshots will be created every five minutes.

This approach provides a simple, durable, and cost-effective recovery mechanism while avoiding the operational complexity of message brokers.

## Consequences

The system will have a maximum expected data loss window of approximately five minutes.

Recovery after failure will be faster because the application can load the latest snapshot instead of rebuilding the full order book from scratch.

The system must implement snapshot validation. A snapshot should not be considered valid unless it was fully written and can be parsed successfully.

The system should use compression, for example gzip or zstd, because raw order book data can be large.

The system should avoid overwriting the latest snapshot directly. Instead, it should write snapshots as immutable timestamped files. This reduces the risk of corrupting the latest recoverable state.

The system should optionally maintain a small `latest.json` or `latest.txt` pointer file per exchange and pair after a snapshot upload succeeds.

Example:

```text
order-books/exchange=binance/pair=BTC-USDT/latest.json
```

The pointer file may contain:

```json
{
  "latestSnapshot": "order-books/exchange=binance/pair=BTC-USDT/date=2026-05-10/snapshot-2026-05-10T14-25-00Z.json.gz",
  "createdAt": "2026-05-10T14:25:00Z",
  "checksum": "sha256:..."
}
```

## Implementation Notes

Snapshots should be uploaded atomically from the application perspective.

Recommended flow:

1. Serialize the current order book.
2. Compress the serialized data.
3. Calculate checksum.
4. Upload the snapshot to Azure Blob Storage using a timestamped name.
5. Verify upload success.
6. Write or update the `latest.json` pointer.
7. On startup, read `latest.json`.
8. Download the referenced snapshot.
9. Validate checksum and schema version.
10. Restore the order book into application memory.

If `latest.json` is missing or invalid, the application should search for the newest valid snapshot by timestamp.

The application should log every snapshot upload and recovery attempt.

## Failure Handling

If snapshot upload fails, the application should continue running and retry on the next interval.

If recovery from the latest snapshot fails, the application should try the previous valid snapshot.

If no valid snapshot exists, the application should start from an empty order book or rebuild from the exchange API, depending on the project’s recovery strategy.

## Security

Snapshots may contain sensitive trading or market data. Access to the Blob Storage container should be restricted.

Recommended controls:

- Use managed identity where possible.
- Avoid storing storage account keys in application configuration.
- Enable encryption at rest.
- Restrict access using least privilege.
- Configure retention and lifecycle policies.
- Consider private endpoints if the system runs inside Azure.

## Alternatives for Future Consideration

If the project later requires near-zero data loss, the current snapshot approach may not be enough.

A stronger future design would combine:

- Periodic snapshots in Azure Blob Storage.
- Incremental order book updates written to an append-only event log.
- Recovery by loading the latest snapshot and replaying events after that snapshot.

This hybrid approach is more complex but provides better recovery precision.

Possible technologies for the event log include Kafka, Azure Event Hubs, Azure Queue Storage, or append-only files in Blob Storage.