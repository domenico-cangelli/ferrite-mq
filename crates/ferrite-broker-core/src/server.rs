use crate::trie::TopicTrie;
use ferrite_protocol::{ConnectReturnCode, MqttCodec, Packet, QoS, SubAckReturnCode};
use futures::{SinkExt, StreamExt};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, Duration, Instant};
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

type ClientSender = mpsc::Sender<Packet>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// QoS massimo che il broker sa consegnare *verso i subscriber*.
///
/// Oggi i messaggi in uscita partono sempre a QoS 0 (manca il tracking dei PUBACK
/// dei subscriber, Fase 2 della roadmap). Quando la Fase 2 sarà completa basterà
/// portare questa costante a `QoS::AtLeastOnce`.
const MAX_GRANTED_QOS: QoS = QoS::AtMostOnce;

/// QoS concesso a una sottoscrizione: il minore tra quello richiesto e il massimo supportato.
fn granted_qos(requested: QoS) -> QoS {
    requested.min(MAX_GRANTED_QOS)
}

/// Sessione attiva di un client: canale verso la sua connessione + identificativo
/// univoco della *connessione* (non del client_id, che può essere riusato).
pub struct SessionHandle {
    conn_id: u64,
    tx: ClientSender,
}

#[derive(Clone, Default)]
pub struct BrokerState {
    pub trie: Arc<RwLock<TopicTrie>>,
    pub sessions: Arc<RwLock<HashMap<String, SessionHandle>>>,
    next_conn_id: Arc<AtomicU64>,
}

impl BrokerState {
    /// Registra la sessione di un client. Se esiste già una sessione con lo stesso
    /// `client_id` la sostituisce (MQTT-3.1.4-2): il vecchio `Sender` viene distrutto,
    /// il `recv()` della vecchia connessione restituisce `None` e questa si chiude da sola.
    /// Le sottoscrizioni della vecchia sessione vengono scartate (oggi supportiamo solo
    /// clean session).
    ///
    /// Ordine dei lock, da rispettare ovunque servano entrambi: prima `trie`, poi `sessions`.
    async fn register_session(&self, client_id: &str, handle: SessionHandle) {
        let mut trie = self.trie.write().await;
        let mut sessions = self.sessions.write().await;
        if sessions.insert(client_id.to_string(), handle).is_some() {
            trie.remove_client(client_id);
        }
    }

    /// Pulizia a fine connessione. Agisce solo se questa connessione è ancora la
    /// proprietaria del `client_id`: se un'altra l'ha rimpiazzata, sessione e
    /// sottoscrizioni ora appartengono a quella e non vanno toccate.
    async fn release_session(&self, client_id: &str, conn_id: u64, filters: &[String]) {
        let mut trie = self.trie.write().await;
        let mut sessions = self.sessions.write().await;
        let still_owner = sessions
            .get(client_id)
            .is_some_and(|session| session.conn_id == conn_id);
        if still_owner {
            for filter in filters {
                trie.unsubscribe(filter, client_id);
            }
            sessions.remove(client_id);
        }
    }
}

pub async fn run_server(listener: TcpListener) -> Result<(), BoxError> {
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
) -> Result<(), BoxError> {
    let mut framed = Framed::new(stream, MqttCodec::default());

    // 1. Handshake iniziale: il primo pacchetto deve essere un CONNECT
    let (client_id, keep_alive) = match framed.next().await {
        Some(Ok(Packet::Connect {
            client_id,
            keep_alive,
            ..
        })) => {
            // Specifica MQTT: timeout effettivo = keep_alive * 1.5; keep_alive = 0 lo disabilita
            let timeout = (keep_alive > 0).then(|| Duration::from_millis(keep_alive as u64 * 1500));
            (client_id, timeout)
        }
        _ => return Ok(()),
    };

    // 2. Registriamo la sessione *prima* di inviare il CONNACK: quando il client lo
    // riceve, il broker è già pronto a instradargli i messaggi.
    let conn_id = state.next_conn_id.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel::<Packet>(100);
    state
        .register_session(&client_id, SessionHandle { conn_id, tx })
        .await;

    // 3. Da qui in poi qualsiasi errore (anche propagato con `?`) deve comunque
    // passare dalla pulizia: per questo il ciclo vive in un blocco separato e la
    // pulizia sta dopo, fuori dal blocco. Rust non ha `finally`.
    let mut subscribed_filters: Vec<String> = Vec::new();
    let result = async {
        framed
            .send(Packet::ConnAck {
                session_present: false,
                return_code: ConnectReturnCode::Accepted,
            })
            .await?;
        session_loop(
            &mut framed,
            &mut rx,
            &client_id,
            keep_alive,
            &state,
            &mut subscribed_filters,
        )
        .await
    }
    .await;

    state
        .release_session(&client_id, conn_id, &subscribed_filters)
        .await;

    info!("Session cleaned up for [{}]", client_id);
    result
}

async fn session_loop(
    framed: &mut Framed<TcpStream, MqttCodec>,
    rx: &mut mpsc::Receiver<Packet>,
    client_id: &str,
    keep_alive: Option<Duration>,
    state: &BrokerState,
    subscribed_filters: &mut Vec<String>,
) -> Result<(), BoxError> {
    // Watchdog di inattività. Il timer vive *fuori* dal loop e viene riarmato solo quando
    // il client ci scrive qualcosa: il traffico in uscita verso di lui non lo conta.
    let inactivity = sleep(keep_alive.unwrap_or_default());
    tokio::pin!(inactivity);

    loop {
        tokio::select! {
            // Messaggi in uscita dalla coda interna (es. pubblicazioni di altri client)
            outgoing = rx.recv() => {
                match outgoing {
                    Some(packet) => framed.send(packet).await?,
                    None => {
                        // Il Sender è stato distrutto: un nuovo CONNECT ha preso il nostro client_id
                        info!("Client [{}] replaced by a new connection with the same client_id", client_id);
                        return Ok(());
                    }
                }
            }

            // Il client non ha mandato nulla entro keep_alive * 1.5
            _ = &mut inactivity, if keep_alive.is_some() => {
                warn!("Client [{}] timed out (inactivity timeout)", client_id);
                return Ok(());
            }

            // Pacchetti in arrivo dalla rete
            incoming = framed.next() => {
                // Qualsiasi pacchetto ricevuto dal client dimostra che è vivo
                if let Some(duration) = keep_alive {
                    inactivity.as_mut().reset(Instant::now() + duration);
                }

                match incoming {
                    Some(Ok(Packet::PingReq)) => {
                        framed.send(Packet::PingResp).await?;
                    }
                    Some(Ok(Packet::Subscribe { packet_id, topics })) => {
                        // Il write lock sul trie vive solo dentro questo blocco: viene
                        // rilasciato prima dell'I/O di rete, altrimenti un client lento
                        // nel ricevere il SUBACK bloccherebbe tutti gli altri.
                        let return_codes = {
                            let mut trie = state.trie.write().await;
                            topics
                                .into_iter()
                                .map(|topic| {
                                    // Nel trie salviamo il QoS *concesso*, non quello richiesto:
                                    // deve coincidere con quanto comunichiamo nel SUBACK.
                                    let granted = granted_qos(topic.qos);
                                    trie.subscribe(&topic.filter, client_id.to_string(), granted);
                                    subscribed_filters.push(topic.filter);
                                    SubAckReturnCode::from(granted)
                                })
                                .collect::<Vec<_>>()
                        };
                        framed.send(Packet::SubAck { packet_id, return_codes }).await?;
                    }
                    Some(Ok(Packet::Publish {
                        topic,
                        qos,
                        retain,
                        dup,
                        packet_id,
                        payload,
                    })) => {
                        // 1. Regola QoS 1: se il messaggio ricevuto è AtLeastOnce,
                        // dobbiamo confermarne la ricezione al publisher inviando PUBACK con lo stesso ID.
                        if qos == QoS::AtLeastOnce {
                            match packet_id {
                                Some(id) => framed.send(Packet::PubAck { packet_id: id }).await?,
                                None => {
                                    warn!("[{}] Received QoS 1 PUBLISH without packet_id, protocol violation", client_id);
                                    return Ok(());
                                }
                            }
                        }

                        // 2. Lookup nel TopicTrie per trovare chi è iscritto
                        let matched = {
                            let trie = state.trie.read().await;
                            trie.match_topic(&topic)
                        };

                        // 3. Instradamento ai subscriber trovati
                        if !matched.is_empty() {
                            let sessions = state.sessions.read().await;
                            for sub in matched {
                                if let Some(session) = sessions.get(&sub.client_id) {
                                    let _ = session
                                        .tx
                                        .send(Packet::Publish {
                                            topic: topic.clone(),
                                            // Il QoS 1 in uscita non è ancora supportato (servirebbe un
                                            // packet_id dedicato): consegniamo a QoS 0.
                                            qos: QoS::AtMostOnce,
                                            retain,
                                            dup,
                                            packet_id: None,
                                            payload: payload.clone(),
                                        })
                                        .await;
                                }
                            }
                        }
                    }
                    Some(Ok(Packet::Disconnect)) | None => return Ok(()),
                    Some(Err(e)) => {
                        warn!("Protocol error from [{}]: {:?}", client_id, e);
                        return Ok(());
                    }
                    Some(Ok(_)) => {}
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_granted_qos_is_capped_to_broker_maximum() {
        assert_eq!(granted_qos(QoS::AtMostOnce), QoS::AtMostOnce);
        assert!(granted_qos(QoS::AtLeastOnce) <= MAX_GRANTED_QOS);
        assert!(granted_qos(QoS::ExactlyOnce) <= MAX_GRANTED_QOS);
    }

    #[test]
    fn test_suback_code_matches_granted_qos() {
        assert_eq!(
            SubAckReturnCode::from(granted_qos(QoS::ExactlyOnce)),
            SubAckReturnCode::from(MAX_GRANTED_QOS)
        );
    }

    fn handle(conn_id: u64) -> (SessionHandle, mpsc::Receiver<Packet>) {
        let (tx, rx) = mpsc::channel(1);
        (SessionHandle { conn_id, tx }, rx)
    }

    #[tokio::test]
    async fn test_register_replaces_old_session_and_closes_its_channel() {
        let state = BrokerState::default();
        let (first, mut first_rx) = handle(1);
        let (second, _second_rx) = handle(2);

        state.register_session("client", first).await;
        state
            .trie
            .write()
            .await
            .subscribe("old/topic", "client".into(), QoS::AtMostOnce);

        state.register_session("client", second).await;

        // Il vecchio receiver vede il canale chiuso: la vecchia connessione si fermerà.
        assert!(first_rx.recv().await.is_none());
        // Le vecchie sottoscrizioni sono sparite.
        assert!(state.trie.read().await.match_topic("old/topic").is_empty());
        assert_eq!(state.sessions.read().await["client"].conn_id, 2);
    }

    #[tokio::test]
    async fn test_release_by_replaced_connection_keeps_new_session() {
        let state = BrokerState::default();
        let (first, _first_rx) = handle(1);
        let (second, _second_rx) = handle(2);
        state.register_session("client", first).await;
        state.register_session("client", second).await;
        state
            .trie
            .write()
            .await
            .subscribe("new/topic", "client".into(), QoS::AtMostOnce);

        // La vecchia connessione (conn 1) fa pulizia dopo essere stata rimpiazzata
        state
            .release_session("client", 1, &["new/topic".to_string()])
            .await;

        assert!(state.sessions.read().await.contains_key("client"));
        assert_eq!(state.trie.read().await.match_topic("new/topic").len(), 1);

        // La proprietaria legittima (conn 2) fa pulizia davvero
        state
            .release_session("client", 2, &["new/topic".to_string()])
            .await;
        assert!(!state.sessions.read().await.contains_key("client"));
        assert!(state.trie.read().await.match_topic("new/topic").is_empty());
    }
}
