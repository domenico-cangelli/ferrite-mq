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
use tokio::time::{interval, Duration};
use tokio_util::codec::Framed;

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

#[derive(Clone)]
pub struct AsyncClient {
    command_tx: mpsc::Sender<ClientCommand>,
    packet_id_counter: Arc<AtomicU16>,
}

impl AsyncClient {
    pub fn next_packet_id(&self) -> u16 {
        self.packet_id_counter.fetch_add(1, Ordering::Relaxed)
    }

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

    pub async fn disconnect(&self) -> Result<(), ClientError> {
        self.command_tx
            .send(ClientCommand::Disconnect)
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        Ok(())
    }
}

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

pub async fn connect(
    options: MqttOptions,
) -> Result<(AsyncClient, mpsc::Receiver<Packet>), ClientError> {
    let stream = TcpStream::connect(options.broker_addr).await?;
    let mut framed = Framed::new(stream, MqttCodec::default());

    let connect_packet = Packet::Connect {
        protocol_version: ProtocolVersion::Mqtt311,
        clean_session: options.clean_session,
        keep_alive: options.keep_alive,
        client_id: options.client_id,
        username: None,
        password: None,
    };
    framed.send(connect_packet).await?;

    match framed.next().await {
        Some(Ok(Packet::ConnAck {
                    return_code: ConnectReturnCode::Accepted,
                    ..
                })) => {}
        Some(Ok(Packet::ConnAck { return_code, .. })) => {
            return Err(ClientError::ConnectionRefused(return_code));
        }
        Some(Ok(_unexpected)) => {
            return Err(ClientError::Protocol(
                ferrite_protocol::ProtocolError::InvalidPacketType(0),
            ));
        }
        Some(Err(e)) => return Err(ClientError::Protocol(e)),
        None => return Err(ClientError::ConnectionClosed),
    }

    let (command_tx, mut command_rx) = mpsc::channel::<ClientCommand>(100);
    let (incoming_tx, incoming_rx) = mpsc::channel::<Packet>(100);

    let packet_id_counter = Arc::new(AtomicU16::new(1));
    let id_gen = packet_id_counter.clone();

    // Impostiamo l'intervallo di Keep-Alive.
    // Se keep_alive è > 0, inviamo il ping al 75% della finestra temporale (calcolato in millisecondi)
    // garantendo che la durata sia sempre strettamente positiva (> 0 ms).
    let ping_duration = if options.keep_alive > 0 {
        let millis = (options.keep_alive as u64 * 1000 * 3) / 4;
        Duration::from_millis(millis).max(Duration::from_millis(250))
    } else {
        Duration::from_secs(u64::MAX / 2)
    };

    let mut ping_interval = interval(ping_duration);
    // Il primo tick scatta immediatamente: lo consumiamo subito per evitare un PING istantaneo
    ping_interval.tick().await;

    tokio::spawn(async move {
        loop {
            tokio::select! {
                // Heartbeat periodico per mantenere attiva la connessione TCP
                _ = ping_interval.tick(), if options.keep_alive > 0 => {
                    if framed.send(Packet::PingReq).await.is_err() {
                        break;
                    }
                }

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

                Some(incoming) = framed.next() => {
                    match incoming {
                        Ok(packet) => {
                            if incoming_tx.send(packet).await.is_err() {
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