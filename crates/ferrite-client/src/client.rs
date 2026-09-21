use crate::error::ClientError;
use bytes::Bytes;
use ferrite_protocol::{
    ConnectReturnCode, MqttCodec, Packet, ProtocolVersion, QoS, SubscribeTopic,
};
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_util::codec::Framed;

/// Comandi interni inviati dall'interfaccia Client al Driver di connessione
#[derive(Debug)]
pub enum ClientCommand {
    Publish {
        topic: String,
        qos: QoS,
        payload: Bytes,
    },
    Subscribe {
        filter: String,
        qos: QoS,
    },
    Disconnect,
}

/// Handle ad alto livello per interagire con il broker MQTT
#[derive(Clone)]
pub struct AsyncClient {
    command_tx: mpsc::Sender<ClientCommand>,
    packet_id_counter: Arc<AtomicU16>,
}

impl AsyncClient {
    /// Invia un messaggio su uno specifico topic con QoS 0 (Fire & Forget)
    pub async fn publish<T: Into<String>, P: Into<Bytes>>(
        &self,
        topic: T,
        payload: P,
    ) -> Result<(), ClientError> {
        self.command_tx
            .send(ClientCommand::Publish {
                topic: topic.into(),
                qos: QoS::AtMostOnce,
                payload: payload.into(),
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        Ok(())
    }

    /// Sottoscrive un topic filter con il livello di QoS richiesto
    pub async fn subscribe<T: Into<String>>(&self, filter: T, qos: QoS) -> Result<(), ClientError> {
        self.command_tx
            .send(ClientCommand::Subscribe {
                filter: filter.into(),
                qos,
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        Ok(())
    }

    /// Disconnette in modo pulito il client
    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.command_tx
            .send(ClientCommand::Disconnect)
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        Ok(())
    }
}

/// Opzioni di configurazione per il client
#[derive(Debug, Clone)]
pub struct MqttOptions {
    pub client_id: String,
    pub broker_addr: SocketAddr,
    pub keep_alive: u16,
    pub clean_session: bool,
}

impl MqttOptions {
    pub fn new<S: Into<String>>(client_id: S, broker_addr: SocketAddr) -> Self {
        Self {
            client_id: client_id.into(),
            broker_addr,
            keep_alive: 60,
            clean_session: true,
        }
    }
}

/// Stabilisce la connessione con il broker, esegue l'handshake CONNECT/CONNACK
/// e restituisce l'Handle client insieme all'EventLoop per ricevere i messaggi in ingresso.
pub async fn connect(
    options: MqttOptions,
) -> Result<(AsyncClient, mpsc::Receiver<Packet>), ClientError> {
    let stream = TcpStream::connect(options.broker_addr).await?;
    let mut framed = Framed::new(stream, MqttCodec::default());

    // 1. Invio del frame CONNECT
    let connect_packet = Packet::Connect {
        protocol_version: ProtocolVersion::Mqtt311,
        clean_session: options.clean_session,
        keep_alive: options.keep_alive,
        client_id: options.client_id,
        username: None,
        password: None,
    };
    framed.send(connect_packet).await?;

    // 2. Attesa del CONNACK di risposta
    match framed.next().await {
        Some(Ok(Packet::ConnAck {
                    return_code: ConnectReturnCode::Accepted,
                    ..
                })) => {}
        Some(Ok(Packet::ConnAck { return_code, .. })) => {
            return Err(ClientError::ConnectionRefused(return_code));
        }
        Some(Ok(unexpected)) => {
            return Err(ClientError::Protocol(
                ferrite_protocol::ProtocolError::InvalidPacketType(0),
            ));
        }
        Some(Err(e)) => return Err(ClientError::Protocol(e)),
        None => return Err(ClientError::ConnectionClosed),
    }

    // Canale per inoltrare i comandi dall'AsyncClient al Connection Driver
    let (command_tx, mut command_rx) = mpsc::channel::<ClientCommand>(100);

    // Canale per inoltrare i pacchetti ricevuti dalla rete all'applicazione utente
    let (incoming_tx, incoming_rx) = mpsc::channel::<Packet>(100);

    let packet_id_counter = Arc::new(AtomicU16::new(1));
    let id_gen = packet_id_counter.clone();

    // 3. Spawna il Driver asincrono che supervisiona I/O di rete e comandi utente
    tokio::spawn(async move {
        loop {
            tokio::select! {
                // Comandi inviati tramite l'AsyncClient
                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        ClientCommand::Publish { topic, qos, payload } => {
                            let packet = Packet::Publish {
                                topic,
                                qos,
                                retain: false,
                                dup: false,
                                packet_id: None,
                                payload,
                            };
                            if framed.send(packet).await.is_err() {
                                break;
                            }
                        }
                        ClientCommand::Subscribe { filter, qos } => {
                            let packet_id = id_gen.fetch_add(1, Ordering::Relaxed);
                            let packet = Packet::Subscribe {
                                packet_id,
                                topics: vec![SubscribeTopic { filter, qos }],
                            };
                            if framed.send(packet).await.is_err() {
                                break;
                            }
                        }
                        ClientCommand::Disconnect => {
                            let _ = framed.send(Packet::Disconnect).await;
                            break;
                        }
                    }
                }

                // Pacchetti in arrivo dal broker sulla socket TCP
                Some(incoming) = framed.next() => {
                    match incoming {
                        Ok(packet) => {
                            if incoming_tx.send(packet).await.is_err() {
                                // L'utente ha deallocato la coda di ricezione
                                break;
                            }
                        }
                        Err(_) => break,
                    }
                }

                else => break,
            }
        }
    });

    let client = AsyncClient {
        command_tx,
        packet_id_counter,
    };

    Ok((client, incoming_rx))
}