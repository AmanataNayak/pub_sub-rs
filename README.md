# pub_sub_rs

A lightweight, fast Pub/Sub system written in Rust using Tokio and Tonic (gRPC).

`pub_sub_rs` is an in-memory messaging service that supports message streaming, persistent disk storage using a Write-Ahead Log (WAL), Dead Letter Topics (DLT), and HTTP Push Webhooks.

---

## Features

- **Topic & Subscription Model:** Decoupled publishers and subscribers with independent message queues.
- **gRPC Streaming Pull:** Long-polling bidirectional stream using `tokio::sync::Notify` for instant message delivery without busy loops.
- **Push Webhooks:** Deliver messages directly to external HTTP endpoints with custom headers and auto-acknowledgment on HTTP `200 OK`.
- **Write-Ahead Log (WAL):** Appends events to disk (`pubsub.wal`) so topics, subscriptions, and messages recover automatically on restart.
- **Dead Letter Topics (DLT):** Routes poison messages to a dead-letter queue after reaching maximum delivery attempts.
- **Visibility Timeouts:** Unacknowledged messages automatically return to the queue after a configurable deadline expires.
- **Batching:** Pull up to $N$ messages in a single atomic lock operation.

---

## Architecture

```text
[ Publishers ] ──(gRPC / Engine)──> [ Topic ] ──(Fan-out)──> [ Subscriptions ]
                                                                   │
                                                ┌──────────────────┴──────────────────┐
                                                ▼                                     ▼
                                      [ Streaming Pull ]                      [ Push Webhook ]
                                       (gRPC / Python)                       (HTTP POST Endpoint)
