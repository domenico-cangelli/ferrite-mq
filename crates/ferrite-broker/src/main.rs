use ferrite_broker_core::TopicTrie;
use ferrite_protocol::{
    ConnectReturnCode, MqttCodec, Packet, SubAckReturnCode,
};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

/// Canale di comunicazione per inoltrare i pacchetti alla socket di uno specifico client
type ClientSender = mpsc::Sender<Packet>;

/// Stato globale condiviso del broker tra tutti i task TCP
#[derive(Clone, Default)]
pub struct BrokerState {
    /// L'albero gerarchico dei topic
    pub trie: Arc<RwLock<TopicTrie>>,
    /// Registro dei client attualmente connessi con le rispettive code di trasmissione
    pub sessions: Arc<RwLock<HashMap<String, ClientSender>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "ferrite_broker=info".into()),
        )
        .init();

    let bind_addr = "0.0.0.0:1883";
    let listener = TcpListener::bind(bind_addr).await?;
    info!("🚀 FerriteMQ broker running on {}", bind_addr);

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
                warn!("TCP accept failed: {:?}", e);
            }
        }
    }
}

async fn handle_client(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: BrokerState,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut framed = Framed::new(stream, MqttCodec::default());

    // 1. Handshake iniziale: il primo pacchetto DEVE essere CONNECT
    let client_id = match framed.next().await {
        Some(Ok(Packet::Connect { client_id, .. })) => {
            info!("Client [{}] connected from {}", client_id, peer_addr);
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

    // 2. Creazione della cassetta postale del client (buffer da 100 pacchetti)
    let (tx, mut rx) = mpsc::channel::<Packet>(100);

    // Registra la sessione attiva
    {
        let mut sessions = state.sessions.write().await;
        sessions.insert(client_id.clone(), tx);
    }

    // Teniamo traccia dei filtri a cui questo client si è iscritto durante la sessione
    let mut subscribed_filters = Vec::new();

    // 3. Event loop bidirezionale usando `tokio::select!`
    // Questo macro permette di attendere contemporaneamente:
    // - Un pacchetto in arrivo dalla rete (da `framed.next()`)
    // - Un pacchetto da inoltrare verso la rete (da `rx.recv()`)
    loop {
        tokio::select! {
            // Messaggio proveniente dalla coda interna (es. pubblicato da un altro client)
            Some(outgoing_packet) = rx.recv() => {
                framed.send(outgoing_packet).await?;
            }

            // Pacchetto in arrivo dal socket del client
            incoming = framed.next() => {
                match incoming {
                    Some(Ok(Packet::PingReq)) => {
                        framed.send(Packet::PingResp).await?;
                    }

                    Some(Ok(Packet::Subscribe { packet_id, topics })) => {
                        let mut return_codes = Vec::new();

                        // Acquisiamo il lock di scrittura sul TopicTrie
                        let mut trie = state.trie.write().await;
                        for topic in topics {
                            info!("[{}] SUBSCRIBE -> '{}' (QoS: {:?})", client_id, topic.filter, topic.qos);
                            trie.subscribe(&topic.filter, client_id.clone(), topic.qos);
                            subscribed_filters.push(topic.filter);
                            // Confermiamo la subscription con il livello richiesto
                            return_codes.push(SubAckReturnCode::SuccessQoS0);
                        }

                        // Rispondiamo con SUBACK
                        framed.send(Packet::SubAck { packet_id, return_codes }).await?;
                    }

                    Some(Ok(Packet::Publish { topic, qos, retain, dup, packet_id, payload })) => {
                        info!("[{}] PUBLISH -> topic: '{}', payload: {} bytes", client_id, topic, payload.len());

                        // Acquisiamo il lock di sola LETTURA: altri task possono leggere in contemporanea!
                        let matched_subscribers = {
                            let trie = state.trie.read().await;
                            trie.match_topic(&topic)
                        };

                        // Se ci sono subscriber, inoltriamo il messaggio ai loro canali
                        if !matched_subscribers.is_empty() {
                            let sessions = state.sessions.read().await;
                            for sub in matched_subscribers {
                                if let Some(target_tx) = sessions.get(&sub.client_id) {
                                    let packet_to_forward = Packet::Publish {
                                        topic: topic.clone(),
                                        qos: sub.qos,
                                        retain,
                                        dup,
                                        packet_id,
                                        payload: payload.clone(), // Zero-copy clone di Bytes!
                                    };
                                    // Invio non bloccante nella coda del destinatario
                                    let _ = target_tx.send(packet_to_forward).await;
                                }
                            }
                        }
                    }

                    Some(Ok(Packet::Disconnect)) => {
                        info!("Client [{}] disconnected cleanly", client_id);
                        break;
                    }

                    Some(Err(e)) => {
                        warn!("[{}] Protocol error: {:?}", client_id, e);
                        break;
                    }

                    None => {
                        // Socket chiuso dal client
                        break;
                    }

                    _ => {}
                }
            }
        }
    }

    // 4. Pulizia (Cleanup) alla disconnessione
    {
        let mut trie = state.trie.write().await;
        for filter in subscribed_filters {
            trie.unsubscribe(&filter, &client_id);
        }
        let mut sessions = state.sessions.write().await;
        sessions.remove(&client_id);
    }

    info!("Cleaned up session for client [{}]", client_id);
    Ok(())
}