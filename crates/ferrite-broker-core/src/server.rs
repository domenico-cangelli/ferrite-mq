use crate::trie::TopicTrie;
use ferrite_protocol::{ConnectReturnCode, MqttCodec, Packet, SubAckReturnCode};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio_util::codec::Framed;
use tracing::{error, warn};

type ClientSender = mpsc::Sender<Packet>;

#[derive(Clone, Default)]
pub struct BrokerState {
    pub trie: Arc<RwLock<TopicTrie>>,
    pub sessions: Arc<RwLock<HashMap<String, ClientSender>>>,
}

/// Avvia il loop di accettazione connessioni su un listener già aperto
pub async fn run_server(listener: TcpListener) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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

    let client_id = match framed.next().await {
        Some(Ok(Packet::Connect { client_id, .. })) => {
            framed
                .send(Packet::ConnAck {
                    session_present: false,
                    return_code: ConnectReturnCode::Accepted,
                })
                .await?;
            client_id
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
            Some(outgoing) = rx.recv() => {
                framed.send(outgoing).await?;
            }

            incoming = framed.next() => {
                match incoming {
                    Some(Ok(Packet::PingReq)) => {
                        framed.send(Packet::PingResp).await?;
                    }
                    Some(Ok(Packet::Subscribe { packet_id, topics })) => {
                        let mut return_codes = Vec::new();
                        let mut trie = state.trie.write().await;
                        for topic in topics {
                            trie.subscribe(&topic.filter, client_id.clone(), topic.qos);
                            subscribed_filters.push(topic.filter);
                            return_codes.push(SubAckReturnCode::SuccessQoS0);
                        }
                        framed.send(Packet::SubAck { packet_id, return_codes }).await?;
                    }
                    Some(Ok(Packet::Publish { topic, qos: _, retain, dup, packet_id, payload })) => {
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
                    Some(Ok(Packet::Disconnect)) | None => break,
                    _ => {}
                }
            }
        }
    }

    {
        let mut trie = state.trie.write().await;
        for filter in subscribed_filters {
            trie.unsubscribe(&filter, &client_id);
        }
        let mut sessions = state.sessions.write().await;
        sessions.remove(&client_id);
    }

    Ok(())
}