use crate::error::ClientError;
use bytes::Bytes;
use ferrite_protocol::{
    ConnectReturnCode, MqttCodec, Packet, ProtocolVersion, QoS, SubAckReturnCode, SubscribeTopic,
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
        packet_id: u16,
        filter: String,
        qos: QoS,
        ack_sender: oneshot::Sender<Result<QoS, ClientError>>,
    },
    Unsubscribe {
        packet_id: u16,
        filter: String,
        ack_sender: oneshot::Sender<()>,
    },
    Disconnect,
}

/// Tabelle delle richieste in volo del driver client (PUBACK, SUBACK, UNSUBACK).
/// Centralizza la pulizia delle voci cancellate e la scelta di un packet ID libero
/// su tutte e tre le tabelle in un unico punto (MQTT-2.3.1-2).
#[derive(Default)]
struct PendingTables {
    pubacks: HashMap<u16, oneshot::Sender<()>>,
    subacks: HashMap<u16, oneshot::Sender<Result<QoS, ClientError>>>,
    unsubacks: HashMap<u16, oneshot::Sender<()>>,
}

impl PendingTables {
    fn allocate_id(&mut self, counter: &AtomicU16, preferred: u16) -> Option<u16> {
        self.pubacks.retain(|_, tx| !tx.is_closed());
        self.subacks.retain(|_, tx| !tx.is_closed());
        self.unsubacks.retain(|_, tx| !tx.is_closed());

        free_packet_id(counter, preferred, |id| {
            self.pubacks.contains_key(&id)
                || self.subacks.contains_key(&id)
                || self.unsubacks.contains_key(&id)
        })
    }
}
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

/// Sceglie un Packet ID libero nell'intervallo [1, 65535] che non risulti occupato da `is_busy`.
///
/// Se `preferred` è valido (!= 0) e libero, lo restituisce subito. Altrimenti avanza `counter`
/// con `next_id` (che salta sempre lo 0) al massimo 65535 volte. Ritorna `None` se tutti gli
/// ID sono attualmente in volo (MQTT-2.3.1-2).
fn free_packet_id<F>(counter: &AtomicU16, preferred: u16, is_busy: F) -> Option<u16>
where
    F: Fn(u16) -> bool,
{
    if preferred != 0 && !is_busy(preferred) {
        return Some(preferred);
    }
    for _ in 0..u16::MAX {
        let candidate = next_id(counter);
        if !is_busy(candidate) {
            return Some(candidate);
        }
    }
    None
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

    /// Sottoscrive un topic filter con il livello di QoS specificato e attende il SUBACK dal broker,
    /// restituendo il QoS effettivamente concesso oppure [`ClientError::SubscriptionRefused`].
    pub async fn subscribe<T: Into<String>>(
        &self,
        filter: T,
        qos: QoS,
    ) -> Result<QoS, ClientError> {
        let packet_id = self.next_packet_id();
        let (ack_tx, ack_rx) = oneshot::channel();

        self.command_tx
            .send(ClientCommand::Subscribe {
                packet_id,
                filter: filter.into(),
                qos,
                ack_sender: ack_tx,
            })
            .await
            .map_err(|_| ClientError::ChannelClosed)?;

        ack_rx.await.map_err(|_| ClientError::AckChannelDropped)?
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

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Connette il client al broker MQTT e avvia il driver di rete in background.
///
/// Restituisce l'handle [`AsyncClient`] per inviare comandi e il ricevitore `mpsc::Receiver<Packet>`
/// per i pacchetti in ingresso. Quando tutti gli handle `AsyncClient` (inclusi eventuali clone)
/// vengono distrutti, il driver invia `DISCONNECT` al broker e termina, chiudendo anche il canale
/// `incoming` (anche se il ricevitore è ancora tenuto vivo dal chiamante).
pub async fn connect(
    options: MqttOptions,
) -> Result<(AsyncClient, mpsc::Receiver<Packet>), ClientError> {
    connect_with_timeout(options, CONNECT_TIMEOUT).await
}

async fn connect_with_timeout(
    options: MqttOptions,
    connect_timeout: Duration,
) -> Result<(AsyncClient, mpsc::Receiver<Packet>), ClientError> {
    let broker_addr = options.broker_addr;
    let keep_alive = options.keep_alive;
    let connect_packet = Packet::Connect {
        protocol_version: ProtocolVersion::Mqtt311,
        clean_session: options.clean_session,
        keep_alive,
        client_id: options.client_id,
        will: None,
        username: None,
        password: None,
    };

    let mut framed = tokio::time::timeout(connect_timeout, async {
        let stream = TcpStream::connect(broker_addr).await?;
        let mut framed = Framed::new(stream, MqttCodec::default());
        framed.send(connect_packet).await?;

        match framed.next().await {
            Some(Ok(Packet::ConnAck {
                return_code: ConnectReturnCode::Accepted,
                ..
            })) => Ok(framed),
            Some(Ok(Packet::ConnAck { return_code, .. })) => {
                Err(ClientError::ConnectionRefused(return_code))
            }
            Some(Ok(_unexpected)) => Err(ClientError::Protocol(
                ferrite_protocol::ProtocolError::InvalidPacketType(0),
            )),
            Some(Err(e)) => Err(ClientError::Protocol(e)),
            None => Err(ClientError::ConnectionClosed),
        }
    })
    .await
    .map_err(|_| {
        ClientError::Io(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "connect timeout",
        ))
    })??;

    let (command_tx, mut command_rx) = mpsc::channel::<ClientCommand>(100);
    let (incoming_tx, incoming_rx) = mpsc::channel::<Packet>(100);

    let packet_id_counter = Arc::new(AtomicU16::new(1));
    let id_gen = packet_id_counter.clone();

    let mut ping_interval = if keep_alive > 0 {
        let millis = (keep_alive as u64 * 1000 * 3) / 4;
        let mut iv = interval(Duration::from_millis(millis).max(Duration::from_millis(250)));
        iv.tick().await;
        Some(iv)
    } else {
        None
    };

    tokio::spawn(async move {
        let mut pending = PendingTables::default();

        loop {
            tokio::select! {
                _ = async {
                    match ping_interval.as_mut() {
                        Some(iv) => {
                            iv.tick().await;
                        }
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    if framed.send(Packet::PingReq).await.is_err() {
                        break;
                    }
                }

                cmd = command_rx.recv() => {
                    match cmd {
                        Some(ClientCommand::PublishQoS0 { topic, payload }) => {
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
                        Some(ClientCommand::PublishQoS1 { packet_id, topic, payload, ack_sender }) => {
                            let Some(packet_id) = pending.allocate_id(&id_gen, packet_id) else {
                                warn!("Dropping PublishQoS1: all 65535 MQTT packet IDs are in flight");
                                continue;
                            };
                            pending.pubacks.insert(packet_id, ack_sender);
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
                        Some(ClientCommand::Subscribe {
                            packet_id,
                            filter,
                            qos,
                            ack_sender,
                        }) => {
                            let Some(packet_id) = pending.allocate_id(&id_gen, packet_id) else {
                                warn!("Dropping Subscribe: all 65535 MQTT packet IDs are in flight");
                                continue;
                            };
                            pending.subacks.insert(packet_id, ack_sender);
                            let packet = Packet::Subscribe {
                                packet_id,
                                topics: vec![SubscribeTopic { filter, qos }],
                            };
                            if framed.send(packet).await.is_err() {
                                break;
                            }
                        }
                        Some(ClientCommand::Unsubscribe {
                            packet_id,
                            filter,
                            ack_sender,
                        }) => {
                            let Some(packet_id) = pending.allocate_id(&id_gen, packet_id) else {
                                warn!("Dropping Unsubscribe: all 65535 MQTT packet IDs are in flight");
                                continue;
                            };
                            pending.unsubacks.insert(packet_id, ack_sender);
                            let packet = Packet::Unsubscribe {
                                packet_id,
                                filters: vec![filter],
                            };
                            if framed.send(packet).await.is_err() {
                                break;
                            }
                        }
                        Some(ClientCommand::Disconnect) | None => {
                            // Tutti gli handle sono stati distrutti o è stato chiesto Disconnect:
                            // inviamo DISCONNECT (MQTT-3.14.4-3) e terminiamo il driver.
                            let _ = framed.send(Packet::Disconnect).await;
                            break;
                        }
                    }
                }

                incoming = framed.next() => {
                    match incoming {
                        Some(Ok(Packet::PubAck { packet_id })) => {
                            // Se un oneshot::Sender è registrato per questo ID, inviamo la notifica
                            if let Some(ack_tx) = pending.pubacks.remove(&packet_id) {
                                let _ = ack_tx.send(());
                            }
                        }
                        Some(Ok(Packet::SubAck {
                            packet_id,
                            return_codes,
                        })) => {
                            // SUBACK è sempre consumato dal driver (anche se packet_id sconosciuto).
                            if let Some(ack_tx) = pending.subacks.remove(&packet_id) {
                                if return_codes.len() != 1 {
                                    warn!(
                                        "SUBACK for packet_id {} returned {} codes (expected 1), dropping ack",
                                        packet_id,
                                        return_codes.len()
                                    );
                                } else {
                                    let result = match return_codes[0] {
                                        SubAckReturnCode::SuccessQoS0 => Ok(QoS::AtMostOnce),
                                        SubAckReturnCode::SuccessQoS1 => Ok(QoS::AtLeastOnce),
                                        SubAckReturnCode::SuccessQoS2 => Ok(QoS::ExactlyOnce),
                                        SubAckReturnCode::Failure => {
                                            Err(ClientError::SubscriptionRefused)
                                        }
                                    };
                                    let _ = ack_tx.send(result);
                                }
                            }
                        }
                        Some(Ok(Packet::UnsubAck { packet_id })) => {
                            // Risolviamo il oneshot senza inoltrare UNSUBACK al canale incoming
                            if let Some(ack_tx) = pending.unsubacks.remove(&packet_id) {
                                let _ = ack_tx.send(());
                            }
                        }
                        Some(Ok(Packet::PingResp)) => {
                            // PINGRESP è un dettaglio interno del keep-alive: consumato dal driver.
                        }
                        Some(Ok(packet)) => {
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
                        Some(Err(_)) | None => break,
                    }
                }
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

    async fn accept_and_connack(
        listener: &tokio::net::TcpListener,
    ) -> Framed<TcpStream, MqttCodec> {
        let (stream, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(stream, MqttCodec::default());
        let first = framed.next().await.unwrap().unwrap();
        assert!(matches!(first, Packet::Connect { .. }));
        framed
            .send(Packet::ConnAck {
                session_present: false,
                return_code: ConnectReturnCode::Accepted,
            })
            .await
            .unwrap();
        framed
    }

    #[tokio::test]
    async fn test_dropping_all_handles_sends_disconnect_and_closes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = tokio::spawn(async move {
            let mut framed = accept_and_connack(&listener).await;
            let pkt = tokio::time::timeout(Duration::from_secs(2), framed.next())
                .await
                .expect("il finto broker non ha ricevuto DISCONNECT entro 2s")
                .expect("connessione chiusa senza DISCONNECT")
                .expect("errore codec");
            assert_eq!(pkt, Packet::Disconnect);

            let eof = tokio::time::timeout(Duration::from_secs(2), framed.next())
                .await
                .expect("il socket non si è chiuso dopo DISCONNECT");
            assert!(eof.is_none());
        });

        let mut opts = MqttOptions::new("drop-handle", addr);
        opts.keep_alive = 60;
        let (client, _incoming_rx) = connect(opts).await.unwrap();

        // Distruggiamo l'unico handle: il driver deve inviare DISCONNECT e terminare.
        drop(client);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn test_broker_eof_wakes_pending_qos1_and_closes_incoming() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = tokio::spawn(async move {
            let mut framed = accept_and_connack(&listener).await;
            // Legge il PUBLISH QoS 1 e chiude il socket senza rispondere PUBACK (EOF).
            let pkt = framed.next().await.unwrap().unwrap();
            assert!(matches!(pkt, Packet::Publish { .. }));
            drop(framed);
        });

        let mut opts = MqttOptions::new("eof-client", addr);
        opts.keep_alive = 60;
        let (client, mut incoming_rx) = connect(opts).await.unwrap();

        let pub_res = tokio::time::timeout(
            Duration::from_secs(2),
            client.publish_qos1("t/eof", Bytes::from_static(b"hi")),
        )
        .await
        .expect("publish_qos1 è rimasto appeso dopo EOF del broker");
        assert!(pub_res.is_err());

        let incoming_closed = tokio::time::timeout(Duration::from_secs(2), incoming_rx.recv())
            .await
            .expect("incoming_rx non si è chiuso dopo EOF del broker");
        assert!(incoming_closed.is_none());

        broker.await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_timeout_when_broker_never_responds() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        // Finto broker che accetta il TCP ma non risponde mai.
        let _broker = tokio::spawn(async move {
            let (_stream, _) = listener.accept().await.unwrap();
            std::future::pending::<()>().await;
        });

        let opts = MqttOptions::new("silent-broker", addr);
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            connect_with_timeout(opts, Duration::from_millis(150)),
        )
        .await
        .expect("connect_with_timeout non ha rispettato il timeout");

        match res {
            Err(ClientError::Io(e)) => assert_eq!(e.kind(), std::io::ErrorKind::TimedOut),
            Ok(_) => panic!("atteso errore TimedOut, ottenuto Ok"),
            Err(other) => panic!("atteso ClientError::Io(TimedOut), ottenuto {:?}", other),
        }
    }

    #[tokio::test]
    async fn test_keep_alive_zero_sends_no_pingreq() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = tokio::spawn(async move {
            let mut framed = accept_and_connack(&listener).await;
            let unexpected = tokio::time::timeout(Duration::from_millis(1500), framed.next()).await;
            assert!(
                unexpected.is_err(),
                "con keep_alive = 0 il client non deve inviare PINGREQ, ricevuto: {:?}",
                unexpected
            );
        });

        let mut opts = MqttOptions::new("no-keepalive", addr);
        opts.keep_alive = 0;
        let (_client, _incoming_rx) = connect(opts).await.unwrap();
        broker.await.unwrap();
    }

    #[test]
    fn test_free_packet_id_selection_wrap_and_exhaustion() {
        // 1. Preferito libero -> restituito subito
        let counter = AtomicU16::new(10);
        assert_eq!(free_packet_id(&counter, 5, |_| false), Some(5));

        // 2. Preferito occupato -> sceglie il primo libero dal contatore
        let counter = AtomicU16::new(2);
        assert_eq!(
            free_packet_id(&counter, 1, |id| id == 1 || id == 2),
            Some(3)
        );

        // 3. Wrap oltre 65535 -> salta sempre 0 e riparte da 1
        let counter = AtomicU16::new(u16::MAX);
        assert_eq!(
            free_packet_id(&counter, u16::MAX, |id| id == u16::MAX),
            Some(1)
        );

        // 4. Tutti i 65535 ID occupati -> None
        let counter = AtomicU16::new(1);
        assert_eq!(free_packet_id(&counter, 1, |_| true), None);
    }

    #[tokio::test]
    async fn test_subscribe_waits_for_suback_and_returns_granted_qos() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (sub_received_tx, sub_received_rx) = oneshot::channel::<u16>();
        let (release_suback_tx, release_suback_rx) = oneshot::channel::<()>();

        let broker = tokio::spawn(async move {
            let mut framed = accept_and_connack(&listener).await;
            let pkt = framed.next().await.unwrap().unwrap();
            let packet_id = match pkt {
                Packet::Subscribe { packet_id, .. } => packet_id,
                other => panic!("atteso Subscribe, ricevuto {:?}", other),
            };
            let _ = sub_received_tx.send(packet_id);
            let _ = release_suback_rx.await;

            // Il client ha chiesto QoS 1, il broker concede QoS 0.
            framed
                .send(Packet::SubAck {
                    packet_id,
                    return_codes: vec![ferrite_protocol::SubAckReturnCode::SuccessQoS0],
                })
                .await
                .unwrap();
            let _ = framed.next().await;
        });

        let (client, _incoming_rx) = connect(MqttOptions::new("sub-wait", addr)).await.unwrap();
        let client_clone = client.clone();
        let mut sub_handle =
            tokio::spawn(async move { client_clone.subscribe("a/b", QoS::AtLeastOnce).await });

        // Aspettiamo che il finto broker abbia ricevuto la SUBSCRIBE.
        let _pid = tokio::time::timeout(Duration::from_secs(2), sub_received_rx)
            .await
            .expect("il broker non ha ricevuto SUBSCRIBE")
            .unwrap();

        // Finché il broker non invia SUBACK, subscribe() NON deve completarsi.
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut sub_handle)
                .await
                .is_err(),
            "subscribe() si è completata prima dell'arrivo del SUBACK"
        );

        // Ora il broker invia SUBACK con QoS 0: subscribe() deve completarsi con Ok(QoS::AtMostOnce).
        let _ = release_suback_tx.send(());
        let granted = tokio::time::timeout(Duration::from_secs(2), sub_handle)
            .await
            .expect("subscribe() non si è sbloccata dopo SUBACK")
            .unwrap()
            .expect("subscribe() fallita");
        assert_eq!(granted, QoS::AtMostOnce);

        drop(client);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_failure_returns_subscription_refused() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = tokio::spawn(async move {
            let mut framed = accept_and_connack(&listener).await;
            let packet_id = match framed.next().await.unwrap().unwrap() {
                Packet::Subscribe { packet_id, .. } => packet_id,
                other => panic!("atteso Subscribe, ricevuto {:?}", other),
            };
            framed
                .send(Packet::SubAck {
                    packet_id,
                    return_codes: vec![ferrite_protocol::SubAckReturnCode::Failure],
                })
                .await
                .unwrap();
            let _ = framed.next().await;
        });

        let (client, _incoming_rx) = connect(MqttOptions::new("sub-refuse", addr)).await.unwrap();
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            client.subscribe("a/b", QoS::AtMostOnce),
        )
        .await
        .expect("timeout su subscribe()");
        assert!(
            matches!(res, Err(ClientError::SubscriptionRefused)),
            "atteso Err(SubscriptionRefused), ottenuto {:?}",
            res
        );

        drop(client);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_returns_ack_channel_dropped_on_broker_eof() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = tokio::spawn(async move {
            let mut framed = accept_and_connack(&listener).await;
            let _ = framed.next().await;
            // Chiude senza rispondere SUBACK.
            drop(framed);
        });

        let (client, _incoming_rx) = connect(MqttOptions::new("sub-eof", addr)).await.unwrap();
        let res = tokio::time::timeout(
            Duration::from_secs(2),
            client.subscribe("a/b", QoS::AtMostOnce),
        )
        .await
        .expect("timeout su subscribe()");
        assert!(
            matches!(res, Err(ClientError::AckChannelDropped)),
            "atteso Err(AckChannelDropped), ottenuto {:?}",
            res
        );

        drop(client);
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_does_not_forward_suback_to_incoming() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let broker = tokio::spawn(async move {
            let mut framed = accept_and_connack(&listener).await;
            let packet_id = match framed.next().await.unwrap().unwrap() {
                Packet::Subscribe { packet_id, .. } => packet_id,
                other => panic!("atteso Subscribe, ricevuto {:?}", other),
            };
            framed
                .send(Packet::SubAck {
                    packet_id,
                    return_codes: vec![ferrite_protocol::SubAckReturnCode::SuccessQoS0],
                })
                .await
                .unwrap();
            let _ = framed.next().await;
        });

        let (client, mut incoming_rx) =
            connect(MqttOptions::new("sub-no-fwd", addr)).await.unwrap();
        let granted = tokio::time::timeout(
            Duration::from_secs(2),
            client.subscribe("a/b", QoS::AtMostOnce),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(granted, QoS::AtMostOnce);

        let unexpected = tokio::time::timeout(Duration::from_millis(200), incoming_rx.recv()).await;
        assert!(
            unexpected.is_err(),
            "SUBACK non deve apparire su incoming_rx, ricevuto: {:?}",
            unexpected
        );

        drop(client);
        broker.await.unwrap();
    }
}
