use ferrite_protocol::{ConnectReturnCode, MqttCodec, Packet};
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Inizializza il logger da console (impostabile con RUST_LOG=info o debug)
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ferrite_broker=info".into()),
        )
        .init();

    let bind_addr = "0.0.0.0:1883";
    let listener = TcpListener::bind(bind_addr).await?;
    info!("🚀 FerriteMQ broker running on {}", bind_addr);

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                // Ogni client viene isolato in un task green-thread Tokio indipendente
                tokio::spawn(async move {
                    if let Err(e) = handle_client(socket, peer_addr).await {
                        error!("Connection error with {}: {:?}", peer_addr, e);
                    }
                });
            }
            Err(e) => {
                warn!("TCP accept failed: {:?}", e);
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    peer_addr: SocketAddr,
) -> Result<(), Box<dyn std::error::Error>> {
    // Avvolgiamo la socket TCP con il nostro Codec
    let mut framed = Framed::new(stream, MqttCodec::default());

    // Regola specifica MQTT: il PRIMO pacchetto DEVE essere un CONNECT
    let client_id = match framed.next().await {
        Some(Ok(Packet::Connect {
                    client_id,
                    protocol_version,
                    clean_session,
                    keep_alive,
                    ..
                })) => {
            info!(
                "Client [{}] connected from {} (Protocol: {:?}, CleanSession: {}, KeepAlive: {}s)",
                client_id, peer_addr, protocol_version, clean_session, keep_alive
            );

            // Rispondiamo con CONNACK accettato
            let connack = Packet::ConnAck {
                session_present: false,
                return_code: ConnectReturnCode::Accepted,
            };
            framed.send(connack).await?;
            client_id
        }
        Some(Ok(unexpected)) => {
            warn!(
                "First packet from {} was not CONNECT: {:?}",
                peer_addr, unexpected
            );
            return Ok(());
        }
        Some(Err(e)) => {
            warn!("Protocol error during handshake from {}: {:?}", peer_addr, e);
            return Ok(());
        }
        None => {
            // Connessione chiusa subito dopo l'apertura socket
            return Ok(());
        }
    };

    // Main event loop per questa connessione
    while let Some(result) = framed.next().await {
        match result {
            Ok(Packet::PingReq) => {
                // Risposta immediata al heartbeat di keep-alive
                framed.send(Packet::PingResp).await?;
            }
            Ok(Packet::Publish {
                   topic,
                   qos,
                   retain,
                   dup,
                   packet_id,
                   payload,
               }) => {
                info!(
                    "[{}] PUBLISH -> topic: '{}', QoS: {:?}, payload_bytes: {}",
                    client_id,
                    topic,
                    qos,
                    payload.len()
                );
                // NOTA: il routing multi-client e il salvataggio dei messaggi
                // andranno nel prossimo step dentro `ferrite-broker-core`
            }
            Ok(Packet::Disconnect) => {
                info!("Client [{}] disconnected cleanly", client_id);
                break;
            }
            Ok(unhandled) => {
                warn!("[{}] Unhandled packet: {:?}", client_id, unhandled);
            }
            Err(e) => {
                warn!("[{}] Frame decoding error: {:?}", client_id, e);
                break;
            }
        }
    }

    Ok(())
}