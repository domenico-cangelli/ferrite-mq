# FerriteMQ

An MQTT 3.1.1 suite written in Rust: protocol codec, broker and async client, built on [Tokio](https://tokio.rs).

> **Status: early development.** Not ready for production. The APIs will change.

## What works today

| Area | Status |
| :--- | :--- |
| Codec: `CONNECT` (with Will), `CONNACK`, `PUBLISH`, `PUBACK`, `SUBSCRIBE`, `SUBACK`, `PINGREQ`, `PINGRESP`, `DISCONNECT` | ✅ |
| Topic routing with `+` and `#` wildcards | ✅ |
| Keep-alive watchdog (1.5 × keep-alive) and automatic client heartbeat | ✅ |
| QoS 0 publish/subscribe | ✅ |
| QoS 1 publish (client waits for `PUBACK`) | ✅ |
| Takeover of a duplicate `client_id` | ✅ |
| QoS 1 delivery to subscribers, persistent sessions, retained messages, Last Will | planned |
| QoS 2, TLS, authentication and ACLs | planned |

See [ROADMAP.md](ROADMAP.md) for the detailed plan and the known issues.

## Workspace layout

| Crate | Purpose |
| :--- | :--- |
| `ferrite-protocol` | Packet types, Variable Byte Integer and the `tokio_util` codec |
| `ferrite-broker-core` | Topic trie, session handling and `run_server(TcpListener)` |
| `ferrite-broker` | Standalone broker binary (port `1883`) |
| `ferrite-client` | Async client (`AsyncClient`) |
| `ferrite-cli` | Command line tool (placeholder) |

## Quick start

Run the broker:

```sh
cargo run -p ferrite-broker
```

Use the client from Rust:

```rust
use ferrite_client::{connect, MqttOptions, QoS};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "127.0.0.1:1883".parse()?;
    let (client, mut incoming) = connect(MqttOptions::new("my-client", addr)).await?;

    client.subscribe("sensors/+/temperature", QoS::AtMostOnce).await?;
    client.publish_qos1("sensors/kitchen/temperature", "21.5").await?;

    while let Some(packet) = incoming.recv().await {
        println!("{packet:?}");
    }
    Ok(())
}
```

## Development

```sh
cargo test --workspace
cargo clippy --workspace --all-targets
```

Set `RUST_LOG=ferrite_broker=debug` to see more broker logs.

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or [MIT license](LICENSE-MIT) at your option.
