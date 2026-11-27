use crate::error::ClientError;
use bytes::Bytes;
use ferrite_protocol::{
    ConnectReturnCode, MqttCodec, Packet, ProtocolVersion, QoS, SubscribeTopic,
};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{interval, Duration};
use tokio_util::codec::Framed;
use tracing::warn;

/// Comandi interni inviati dall'interfaccia Client al Driver di connessione
#[derive(Debug)]
pub enum ClientCommand {
    PublishQoS0 {
        topic: String,
        payload: Bytes,
    },
    PublishQoS1 {
        packet_id: u16,
        topic: String,
        payload: Bytes,
        ack_sender: oneshot::Sender<()>,
    },
    Subscribe {
        filter: String,
        qos: QoS,
    },
    Unsubscribe {
        packet_id: u16,
        filter: String,
        ack_sender: oneshot::Sender<()>,
    },
    Disconnect,
}

/// Handle ad alto livello per interagire con il broker MQTT
#[derive(Clone)]
pub struct AsyncClient {
    command_tx: mpsc::Sender<ClientCommand>,
    packet_id_counter: Arc<AtomicU16>,
}

/// Restituisce il prossimo Packet ID MQTT, nell'intervallo [1, 65535].
///
/// Usa `try_update` (compare-and-swap in un ciclo) così lettura e aggiornamento
/// del contatore sono un'unica operazione atomica: due thread concorrenti non
/// possono mai ottenere lo stesso ID, e il valore 0 (invalido in MQTT) non viene
/// mai memorizzato nel contatore, quindi non può mai essere restituito.
fn next_id(counter: &AtomicU16) -> u16 {
    counter
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            // Dopo 65535 si riparte da 1, saltando lo 0.
            Some(if current == u16::MAX { 1 } else { current + 1 })
        })
        // La closure ritorna sempre Some, quindi try_update non può fallire.
        // Il valore restituito è quello *precedente*, cioè l'ID da usare.
        .expect("la closure restituisce sempre Some")
}

impl AsyncClient {
    pub fn next_packet_id(&self) -> u16 {
        next_id(&self.packet_id_counter)
    }

    /// Invia un messaggio a QoS 0 (Fire & Forget)
    pub async fn publish<T: Into<String>, P: Into<Bytes>>(
        &self,
        topic: T,
        payload: P,
    ) -> Result<(), ClientError> {
        self.command_tx
            .send(ClientCommand::PublishQoS0 {
                topic: topic.into(),
                payload: payload.into(),
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        Ok(())
    }

    /// Invia un messaggio a QoS 1 (At Least Once) e attende il relativo PUBACK dal broker
    pub async fn publish_qos1<T: Into<String>, P: Into<Bytes>>(
        &self,
        topic: T,
        payload: P,
    ) -> Result<(), ClientError> {
        let packet_id = self.next_packet_id();
        let (ack_tx, ack_rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::PublishQoS1 {
                packet_id,
                topic: topic.into(),
                payload: payload.into(),
                ack_sender: ack_tx,
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        // Attende che il Connection Driver riceva il PUBACK corrispondente
        ack_rx.await.map_err(|_| ClientError::AckChannelDropped)?;

        Ok(())
    }

    /// Sottoscrive un topic filter con il livello di QoS specificato
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

    /// Annulla la sottoscrizione a un topic filter e attende il relativo UNSUBACK dal broker
    pub async fn unsubscribe<T: Into<String>>(&self, filter: T) -> Result<(), ClientError> {
        let packet_id = self.next_packet_id();
        let (ack_tx, ack_rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::Unsubscribe {
                packet_id,
                filter: filter.into(),
                ack_sender: ack_tx,
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        // Attende che il Connection Driver riceva l'UNSUBACK corrispondente
        ack_rx.await.map_err(|_| ClientError::AckChannelDropped)?;

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
        will: None,
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

    let ping_duration = if options.keep_alive > 0 {
        let millis = (options.keep_alive as u64 * 1000 * 3) / 4;
        Duration::from_millis(millis).max(Duration::from_millis(250))
    } else {
        Duration::from_secs(u64::MAX / 2)
    };

    let mut ping_interval = interval(ping_duration);
    ping_interval.tick().await;

    tokio::spawn(async move {
        // Mappa dei pacchetti QoS 1 in volo in attesa di PUBACK
        let mut pending_acks: HashMap<u16, oneshot::Sender<()>> = HashMap::new();
        // Mappa delle richieste UNSUBSCRIBE in volo in attesa di UNSUBACK
        let mut pending_unsubacks: HashMap<u16, oneshot::Sender<()>> = HashMap::new();

        loop {
            tokio::select! {
                _ = ping_interval.tick(), if options.keep_alive > 0 => {
                    if framed.send(Packet::PingReq).await.is_err() {
                        break;
                    }
                }

                Some(cmd) = command_rx.recv() => {
                    match cmd {
                        ClientCommand::PublishQoS0 { topic, payload } => {
                            let packet = Packet::Publish {
                                topic,
                                qos: QoS::AtMostOnce,
                                retain: false,
                                dup: false,
                                packet_id: None,
                                payload,
                            };
                            if framed.send(packet).await.is_err() {
                                break;
                            }
                        }
                        ClientCommand::PublishQoS1 { packet_id, topic, payload, ack_sender } => {
                            pending_acks.insert(packet_id, ack_sender);
                            let packet = Packet::Publish {
                                topic,
                                qos: QoS::AtLeastOnce,
                                retain: false,
                                dup: false,
                                packet_id: Some(packet_id),
                                payload,
                            };
                            if framed.send(packet).await.is_err() {
                                break;
                            }
                        }
                        ClientCommand::Subscribe { filter, qos } => {
                            let packet_id = next_id(&id_gen);
                            let packet = Packet::Subscribe {
                                packet_id,
                                topics: vec![SubscribeTopic { filter, qos }],
                            };
                            if framed.send(packet).await.is_err() {
                                break;
                            }
                        }
                        ClientCommand::Unsubscribe {
                            packet_id,
                            filter,
                            ack_sender,
                        } => {
                            pending_unsubacks.insert(packet_id, ack_sender);
                            let packet = Packet::Unsubscribe {
                                packet_id,
                                filters: vec![filter],
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
                        Ok(Packet::PubAck { packet_id }) => {
                            // Se un oneshot::Sender è registrato per questo ID, inviamo la notifica
                            if let Some(ack_tx) = pending_acks.remove(&packet_id) {
                                let _ = ack_tx.send(());
                            }
                        }
                        Ok(Packet::UnsubAck { packet_id }) => {
                            // Risolviamo il oneshot senza inoltrare UNSUBACK al canale incoming
                            if let Some(ack_tx) = pending_unsubacks.remove(&packet_id) {
                                let _ = ack_tx.send(());
                            }
                        }
                        Ok(Packet::PingResp) => {
                            // PINGRESP è un dettaglio interno del keep-alive: consumato dal driver.
                        }
                        Ok(packet) => {
                            // TODO(Fase 2, #15): con QoS 1 in ingresso non si può scartare prima del PUBACK.
                            match incoming_tx.try_send(packet) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!(
                                        "Dropping incoming packet: client incoming channel is full"
                                    );
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {
                                    // Il receiver utente è stato chiuso: il driver resta attivo.
                                }
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;
    use std::thread;

    #[test]
    fn test_packet_id_starts_at_one_and_increments() {
        let counter = AtomicU16::new(1);
        assert_eq!(next_id(&counter), 1);
        assert_eq!(next_id(&counter), 2);
        assert_eq!(next_id(&counter), 3);
    }

    #[test]
    fn test_packet_id_wraps_and_skips_zero() {
        let counter = AtomicU16::new(u16::MAX);
        assert_eq!(next_id(&counter), u16::MAX);
        // Dopo 65535 si riparte da 1: lo 0 non deve mai uscire.
        assert_eq!(next_id(&counter), 1);
        assert_eq!(next_id(&counter), 2);
    }

    #[test]
    fn test_packet_id_never_zero_over_full_cycle() {
        let counter = AtomicU16::new(1);
        for _ in 0..(u16::MAX as u32 * 2) {
            assert_ne!(next_id(&counter), 0);
        }
    }

    #[test]
    fn test_next_id_full_cycle_returns_to_one_without_zero() {
        let counter = AtomicU16::new(1);
        let first = next_id(&counter);
        assert_eq!(first, 1);

        for expected in 2..=u16::MAX {
            let id = next_id(&counter);
            assert_ne!(id, 0);
            assert_eq!(id, expected);
        }

        // Dopo 65535 chiamate, l'ID successivo torna a 1 saltando 0.
        assert_eq!(next_id(&counter), 1);
    }

    #[test]
    fn test_packet_id_unique_across_threads() {
        let counter = Arc::new(AtomicU16::new(1));
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let counter = counter.clone();
                thread::spawn(move || (0..1000).map(|_| next_id(&counter)).collect::<Vec<_>>())
            })
            .collect();

        let mut seen = HashSet::new();
        for handle in handles {
            for id in handle.join().unwrap() {
                assert!(seen.insert(id), "ID duplicato: {id}");
            }
        }
        assert_eq!(seen.len(), 8000);
    }
}
