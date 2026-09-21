use crate::trie::TopicTrie;
use ferrite_protocol::{ConnectReturnCode, MqttCodec, Packet, SubAckReturnCode};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{timeout, Duration};
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

type ClientSender = mpsc::Sender<Packet>;

#[derive(Clone, Default)]
pub struct BrokerState {
    pub trie: Arc<RwLock<TopicTrie>>,
    pub sessions: Arc<RwLock<HashMap<String, ClientSender>>>,
}

pub async fn run_server(
    listener: TcpListener,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = BrokerState::default();

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let state_clone = state.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_client(socket, peer_addr, state_clone).await {
                        error!("Connection error with {}: {:?}", peer_addr, e);
                    }
                });
            }
            Err(e) => {
                warn!("TCP accept error: {:?}", e);
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    _peer_addr: SocketAddr,
    state: BrokerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut framed = Framed::new(stream, MqttCodec::default());

    // 1. Handshake iniziale: estraiamo anche il valore di keep_alive
    let (client_id, keep_alive_duration) = match framed.next().await {
        Some(Ok(Packet::Connect {
                    client_id,
                    keep_alive,
                    ..
                })) => {
            framed
                .send(Packet::ConnAck {
                    session_present: false,
                    return_code: ConnectReturnCode::Accepted,
                })
                .await?;

            // Specifica MQTT: timeout effettivo = keep_alive * 1.5
            let duration = if keep_alive > 0 {
                Duration::from_millis((keep_alive as u64 * 1500).max(500))
            } else {
                Duration::from_secs(u64::MAX / 2) // Keep-alive disabilitato
            };

            (client_id, duration)
        }
        _ => return Ok(()),
    };

    let (tx, mut rx) = mpsc::channel::<Packet>(100);

    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(client_id.clone(), tx);
    }

    let mut subscribed_filters = Vec::new();

    loop {
        tokio::select! {
            // Messaggi in uscita dalla coda interna (es. pubblicazioni di altri)
            Some(outgoing) = rx.recv() => {
                framed.send(outgoing).await?;
            }

            // Pacchetti in arrivo dalla rete protetti da timeout di inattività
            network_event = timeout(keep_alive_duration, framed.next()) => {
                match network_event {
                    // Errore di timeout: il client non ha mandato nulla entro keep_alive * 1.5
                    Err(_) => {
                        warn!("Client [{}] timed out (inactivity timeout)", client_id);
                        break;
                    }

                    // Arrivato un pacchetto valido prima della scadenza
                    Ok(Some(Ok(Packet::PingReq))) => {
                        framed.send(Packet::PingResp).await?;
                    }
                    Ok(Some(Ok(Packet::Subscribe { packet_id, topics }))) => {
                        let mut return_codes = Vec::new();
                        let mut trie = state.trie.write().await;
                        for topic in topics {
                            trie.subscribe(&topic.filter, client_id.clone(), topic.qos);
                            subscribed_filters.push(topic.filter);
                            return_codes.push(SubAckReturnCode::SuccessQoS0);
                        }
                        framed.send(Packet::SubAck { packet_id, return_codes }).await?;
                    }
                    Ok(Some(Ok(Packet::Publish { topic, retain, dup, packet_id, payload, .. }))) => {
                        let matched = {
                            let trie = state.trie.read().await;
                            trie.match_topic(&topic)
                        };

                        if !matched.is_empty() {
                            let sessions = state.sessions.read().await;
                            for sub in matched {
                                if let Some(target_tx) = sessions.get(&sub.client_id) {
                                    let _ = target_tx.send(Packet::Publish {
                                        topic: topic.clone(),
                                        qos: sub.qos,
                                        retain,
                                        dup,
                                        packet_id,
                                        payload: payload.clone(),
                                    }).await;
                                }
                            }
                        }
                    }
                    Ok(Some(Ok(Packet::Disconnect))) | Ok(None) => break,
                    Ok(Some(Err(e))) => {
                        warn!("Protocol error from [{}]: {:?}", client_id, e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    // Pulizia garantita alla chiusura della connessione
    {
        let mut trie = state.trie.write().await;
        for filter in subscribed_filters {
            trie.unsubscribe(&filter, &client_id);
        }
        let mut sessions = state.sessions.write().await;
        sessions.remove(&client_id);
    }

    info!("Session cleaned up for [{}]", client_id);
    Ok(())
}