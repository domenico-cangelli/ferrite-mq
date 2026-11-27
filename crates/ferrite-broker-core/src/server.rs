use crate::trie::{validate_filter, validate_topic_name, TopicTrie};
use ferrite_protocol::{
    ConnectReturnCode, MqttCodec, Packet, ProtocolError, QoS, SubAckReturnCode, SubscribeTopic,
};
use futures::{SinkExt, StreamExt};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::AsyncWrite;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use tokio::time::{sleep, timeout, Duration, Instant};
use tokio_util::codec::Framed;
use tracing::{error, info, warn};

type ClientSender = mpsc::Sender<Packet>;
type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Dimensione massima predefinita di un pacchetto MQTT dopo il `CONNECT` (2 MiB).
const DEFAULT_MAX_PACKET_SIZE: usize = 2 * 1024 * 1024;

/// Pausa dopo un errore di `TcpListener::accept()` (es. EMFILE) per evitare busy-loop al 100% di CPU.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

/// Tempo massimo concesso al client per inviare il pacchetto `CONNECT` iniziale.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Dimensione massima accettata per il pacchetto `CONNECT` iniziale (prima dell'autenticazione).
const PRE_CONNECT_MAX_PACKET_SIZE: usize = 64 * 1024;

/// Configurazione del broker MQTT passata a [`run_server_with_config`].
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Dimensione massima accettata per un singolo pacchetto MQTT dopo il `CONNECT` (default: 2 MiB).
    /// Prima del `CONNECT` vale comunque il minore tra questo valore e 64 KiB.
    pub max_packet_size: usize,
    /// Tempo massimo concesso a una nuova connessione TCP per inviare il pacchetto `CONNECT` (default: 10 s).
    pub connect_timeout: Duration,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            max_packet_size: DEFAULT_MAX_PACKET_SIZE,
            connect_timeout: CONNECT_TIMEOUT,
        }
    }
}

/// Tempo massimo concesso per scrivere un pacchetto sul socket di un client.
/// Se il client non legge e il buffer TCP resta pieno oltre questa soglia,
/// la connessione viene chiusa per evitare task appesi all'infinito.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);

/// Invia un pacchetto sul socket con timeout esplicito.
async fn send_with_timeout<T>(
    framed: &mut Framed<T, MqttCodec>,
    packet: Packet,
    write_timeout: Duration,
) -> Result<(), BoxError>
where
    T: AsyncWrite + Unpin,
{
    match timeout(write_timeout, framed.send(packet)).await {
        Ok(res) => res.map_err(Into::into),
        Err(_) => Err(std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "write timeout on client socket",
        )
        .into()),
    }
}

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

/// Applica i filtri di un pacchetto `SUBSCRIBE` al trie, restituendo un codice SUBACK
/// per ogni filtro nello stesso ordine. I filtri invalidi ricevono `Failure` (0x80)
/// e non vengono aggiunti né al trie né a `filters`.
fn apply_subscribe(
    trie: &mut TopicTrie,
    client_id: &str,
    topics: &[SubscribeTopic],
    filters: &mut HashSet<String>,
) -> Vec<SubAckReturnCode> {
    topics
        .iter()
        .map(|topic| {
            let granted = granted_qos(topic.qos);
            match trie.try_subscribe(&topic.filter, client_id.to_string(), granted) {
                Ok(()) => {
                    filters.insert(topic.filter.clone());
                    SubAckReturnCode::from(granted)
                }
                Err(err) => {
                    warn!(
                        "[{}] Rejecting invalid SUBSCRIBE filter {:?}: {}",
                        client_id, topic.filter, err
                    );
                    SubAckReturnCode::Failure
                }
            }
        })
        .collect()
}

/// Applica le sottoscrizioni solo se `conn_id` è ancora la proprietaria corrente di
/// `client_id`. Prende i lock nell'ordine fisso `trie` (write) -> `sessions` (read)
/// e li rilascia prima di ritornare, senza mai fare I/O sotto lock.
async fn subscribe_if_owner(
    state: &BrokerState,
    client_id: &str,
    conn_id: u64,
    topics: &[SubscribeTopic],
    filters: &mut HashSet<String>,
) -> Option<Vec<SubAckReturnCode>> {
    let mut trie = state.trie.write().await;
    let sessions = state.sessions.read().await;
    let still_owner = sessions
        .get(client_id)
        .is_some_and(|session| session.conn_id == conn_id);
    if !still_owner {
        return None;
    }
    Some(apply_subscribe(&mut trie, client_id, topics, filters))
}

/// Rimuove le sottoscrizioni solo se `conn_id` è ancora la proprietaria corrente di
/// `client_id`. Prende i lock nell'ordine fisso `trie` (write) -> `sessions` (read)
/// e li rilascia prima di ritornare, senza mai fare I/O sotto lock.
async fn unsubscribe_if_owner(
    state: &BrokerState,
    client_id: &str,
    conn_id: u64,
    filters_to_remove: &[String],
    subscribed_filters: &mut HashSet<String>,
) -> Option<()> {
    let mut trie = state.trie.write().await;
    let sessions = state.sessions.read().await;
    let still_owner = sessions
        .get(client_id)
        .is_some_and(|session| session.conn_id == conn_id);
    if !still_owner {
        return None;
    }
    for filter in filters_to_remove {
        trie.unsubscribe(filter, client_id);
        subscribed_filters.remove(filter);
    }
    Some(())
}

/// Raccoglie i destinatari (`client_id`, `ClientSender`) per un `PUBLISH` su `topic`.
async fn collect_targets(state: &BrokerState, topic: &str) -> Vec<(String, ClientSender)> {
    let matched = {
        let trie = state.trie.read().await;
        trie.match_topic(topic)
    };

    if matched.is_empty() {
        return Vec::new();
    }

    let sessions = state.sessions.read().await;
    matched
        .into_iter()
        .filter_map(|sub| {
            sessions
                .get(&sub.client_id)
                .map(|session| (sub.client_id, session.tx.clone()))
        })
        .collect()
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

    /// Genera un `client_id` univoco nel formato `ferrite-auto-<conn_id>` (con suffisso
    /// numerico in caso di collisione con un client già connesso) e registra la sessione
    /// atomicamente sotto lock (MQTT-3.1.3-6).
    async fn register_anonymous_session(&self, handle: SessionHandle) -> String {
        let mut sessions = self.sessions.write().await;
        let base = format!("ferrite-auto-{}", handle.conn_id);
        let client_id = if !sessions.contains_key(&base) {
            base
        } else {
            let mut suffix = 1u64;
            loop {
                let candidate = format!("{base}-{suffix}");
                if !sessions.contains_key(&candidate) {
                    break candidate;
                }
                suffix += 1;
            }
        };
        sessions.insert(client_id.clone(), handle);
        client_id
    }

    /// Pulizia a fine connessione. Agisce solo se questa connessione è ancora la
    /// proprietaria del `client_id`: se un'altra l'ha rimpiazzata, sessione e
    /// sottoscrizioni ora appartengono a quella e non vanno toccate.
    async fn release_session(&self, client_id: &str, conn_id: u64, filters: &HashSet<String>) {
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

/// Avvia il broker MQTT sul `TcpListener` fornito usando la configurazione [`BrokerConfig`] specificata.
pub async fn run_server_with_config(
    listener: TcpListener,
    config: BrokerConfig,
) -> Result<(), BoxError> {
    let state = BrokerState::default();

    loop {
        match listener.accept().await {
            Ok((socket, peer_addr)) => {
                let state_clone = state.clone();
                let config_clone = config.clone();
                tokio::spawn(async move {
                    if let Err(e) =
                        handle_client(socket, peer_addr, state_clone, config_clone).await
                    {
                        error!("Connection error with {}: {:?}", peer_addr, e);
                    }
                });
            }
            Err(e) => {
                warn!("TCP accept error: {:?}", e);
                sleep(ACCEPT_ERROR_BACKOFF).await;
            }
        }
    }
}

/// Avvia il broker MQTT con la configurazione predefinita ([`BrokerConfig::default`]).
pub async fn run_server(listener: TcpListener) -> Result<(), BoxError> {
    run_server_with_config(listener, BrokerConfig::default()).await
}

async fn handle_client(
    stream: TcpStream,
    peer_addr: SocketAddr,
    state: BrokerState,
    config: BrokerConfig,
) -> Result<(), BoxError> {
    // Prima del CONNECT applichiamo il minore tra 64 KiB e `config.max_packet_size`
    // per ridurre il costo delle connessioni anonime non ancora identificate.
    let pre_connect_limit = PRE_CONNECT_MAX_PACKET_SIZE.min(config.max_packet_size);
    let mut framed = Framed::new(stream, MqttCodec::new(pre_connect_limit));

    // 1. Handshake iniziale: il primo pacchetto deve essere un CONNECT entro config.connect_timeout
    let (raw_client_id, clean_session, keep_alive) =
        match timeout(config.connect_timeout, framed.next()).await {
            Ok(Some(Ok(Packet::Connect {
                client_id,
                clean_session,
                keep_alive,
                ..
            }))) => {
                // Specifica MQTT: timeout effettivo = keep_alive * 1.5; keep_alive = 0 lo disabilita
                let keep_alive_timeout =
                    (keep_alive > 0).then(|| Duration::from_millis(keep_alive as u64 * 1500));
                (client_id, clean_session, keep_alive_timeout)
            }
            Ok(Some(Err(ProtocolError::UnsupportedProtocolLevel(level)))) => {
                warn!(
                    "Client {} sent unsupported MQTT protocol level {}, sending CONNACK 0x01",
                    peer_addr, level
                );
                let _ = send_with_timeout(
                    &mut framed,
                    Packet::ConnAck {
                        session_present: false,
                        return_code: ConnectReturnCode::RefusedUnacceptableProtocolVersion,
                    },
                    WRITE_TIMEOUT,
                )
                .await;
                return Ok(());
            }
            Ok(Some(Ok(other))) => {
                warn!(
                "Client {} sent {:?} instead of CONNECT as first packet (MQTT-3.1.0-1), closing",
                peer_addr, other
            );
                return Ok(());
            }
            Ok(Some(Err(e))) => {
                warn!(
                    "Protocol/IO error from {} before CONNECT: {:?}",
                    peer_addr, e
                );
                return Ok(());
            }
            Ok(None) => {
                warn!(
                    "Client {} closed connection before sending CONNECT",
                    peer_addr
                );
                return Ok(());
            }
            Err(_) => {
                warn!("Client {} timed out waiting for initial CONNECT", peer_addr);
                return Ok(());
            }
        };

    // 2. Registriamo la sessione *prima* di inviare il CONNACK: quando il client lo
    // riceve, il broker è già pronto a instradargli i messaggi.
    let conn_id = state.next_conn_id.fetch_add(1, Ordering::Relaxed);
    let (tx, mut rx) = mpsc::channel::<Packet>(100);
    let client_id = if raw_client_id.is_empty() {
        if !clean_session {
            warn!(
                "Client {} sent empty client_id with clean_session=false (MQTT-3.1.3-7), sending CONNACK 0x02",
                peer_addr
            );
            let _ = send_with_timeout(
                &mut framed,
                Packet::ConnAck {
                    session_present: false,
                    return_code: ConnectReturnCode::RefusedIdentifierRejected,
                },
                WRITE_TIMEOUT,
            )
            .await;
            return Ok(());
        }
        let assigned = state
            .register_anonymous_session(SessionHandle { conn_id, tx })
            .await;
        info!(
            "Assigned generated client_id [{}] to anonymous client {}",
            assigned, peer_addr
        );
        assigned
    } else {
        state
            .register_session(&raw_client_id, SessionHandle { conn_id, tx })
            .await;
        raw_client_id
    };

    // 3. Da qui in poi qualsiasi errore (anche propagato con `?`) deve comunque
    // passare dalla pulizia: per questo il ciclo vive in un blocco separato e la
    // pulizia sta dopo, fuori dal blocco. Rust non ha `finally`.
    // Dopo il CONNECT accettato applichiamo il limite configurato per la sessione.
    *framed.codec_mut() = MqttCodec::new(config.max_packet_size);
    let mut subscribed_filters: HashSet<String> = HashSet::new();
    let result = async {
        send_with_timeout(
            &mut framed,
            Packet::ConnAck {
                session_present: false,
                return_code: ConnectReturnCode::Accepted,
            },
            WRITE_TIMEOUT,
        )
        .await?;
        session_loop(
            &mut framed,
            &mut rx,
            &client_id,
            conn_id,
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
    conn_id: u64,
    keep_alive: Option<Duration>,
    state: &BrokerState,
    subscribed_filters: &mut HashSet<String>,
) -> Result<(), BoxError> {
    // Watchdog di inattività. Il timer vive *fuori* dal loop e viene riarmato solo quando
    // il client ci scrive qualcosa: il traffico in uscita verso di lui non lo conta.
    let inactivity = sleep(keep_alive.unwrap_or_default());
    tokio::pin!(inactivity);

    loop {
        // Priorità al takeover senza affamare `framed.next()` o il watchdog: se il Sender
        // della sessione è stato chiuso da una nuova connessione, usciamo subito.
        if rx.is_closed() {
            info!(
                "Client [{}] replaced by a new connection with the same client_id",
                client_id
            );
            return Ok(());
        }

        tokio::select! {
            // Messaggi in uscita dalla coda interna (es. pubblicazioni di altri client)
            outgoing = rx.recv() => {
                match outgoing {
                    Some(packet) => send_with_timeout(framed, packet, WRITE_TIMEOUT).await?,
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
                        send_with_timeout(framed, Packet::PingResp, WRITE_TIMEOUT).await?;
                    }
                    Some(Ok(Packet::Subscribe { packet_id, topics })) => {
                        if topics.is_empty() {
                            warn!(
                                "[{}] Received SUBSCRIBE with empty topic list, protocol violation (MQTT-3.8.3-3)",
                                client_id
                            );
                            return Ok(());
                        }
                        // Il controllo di proprietà e la scrittura nel trie avvengono sotto lock
                        // in `subscribe_if_owner`, che rilascia i lock prima di inviare il SUBACK.
                        let Some(return_codes) = subscribe_if_owner(
                            state,
                            client_id,
                            conn_id,
                            &topics,
                            subscribed_filters,
                        )
                        .await
                        else {
                            info!(
                                "Client [{}] session replaced before applying SUBSCRIBE, closing",
                                client_id
                            );
                            return Ok(());
                        };
                        send_with_timeout(
                            framed,
                            Packet::SubAck {
                                packet_id,
                                return_codes,
                            },
                            WRITE_TIMEOUT,
                        )
                        .await?;
                    }
                    Some(Ok(Packet::Unsubscribe { packet_id, filters })) => {
                        if filters.is_empty() {
                            warn!(
                                "[{}] Received UNSUBSCRIBE with empty filter list, protocol violation (MQTT-3.10.3-2)",
                                client_id
                            );
                            return Ok(());
                        }
                        for filter in &filters {
                            if let Err(err) = validate_filter(filter) {
                                warn!(
                                    "[{}] Received UNSUBSCRIBE with invalid filter {:?} ({}), protocol violation (MQTT-4.8.0-1), closing",
                                    client_id, filter, err
                                );
                                return Ok(());
                            }
                        }
                        if unsubscribe_if_owner(
                            state,
                            client_id,
                            conn_id,
                            &filters,
                            subscribed_filters,
                        )
                        .await
                        .is_none()
                        {
                            info!(
                                "Client [{}] session replaced before applying UNSUBSCRIBE, closing",
                                client_id
                            );
                            return Ok(());
                        }
                        // MQTT-3.10.4-5: UNSUBACK sempre con lo stesso packet_id, anche se il filtro non esisteva.
                        send_with_timeout(framed, Packet::UnsubAck { packet_id }, WRITE_TIMEOUT)
                            .await?;
                    }
                    Some(Ok(Packet::Publish {
                        topic,
                        qos,
                        packet_id,
                        payload,
                        ..
                    })) => {
                        if let Err(err) = validate_topic_name(&topic) {
                            warn!(
                                "[{}] Received PUBLISH with invalid topic {:?} ({}), protocol violation (MQTT-3.3.2-2 / MQTT-4.7.3-1)",
                                client_id, topic, err
                            );
                            return Ok(());
                        }

                        // TODO(Fase 5, #18): implementare handshake QoS 2 (PUBREC/PUBREL/PUBCOMP).
                        if qos == QoS::ExactlyOnce {
                            warn!(
                                "[{}] QoS 2 non supportato (previsto in Fase 5), chiusura connessione",
                                client_id
                            );
                            return Ok(());
                        }

                        // 1. Regola QoS 1: se il messaggio ricevuto è AtLeastOnce,
                        // dobbiamo confermarne la ricezione al publisher inviando PUBACK con lo stesso ID.
                        if qos == QoS::AtLeastOnce {
                            match packet_id {
                                Some(id) => {
                                    send_with_timeout(
                                        framed,
                                        Packet::PubAck { packet_id: id },
                                        WRITE_TIMEOUT,
                                    )
                                    .await?;
                                }
                                None => {
                                    warn!("[{}] Received QoS 1 PUBLISH without packet_id, protocol violation", client_id);
                                    return Ok(());
                                }
                            }
                        }

                        // 2. Raccolta destinatari e instradamento (nessun invio sotto lock)
                        let targets = collect_targets(state, &topic).await;
                        for (target_client_id, tx) in targets {
                            let outgoing = Packet::Publish {
                                topic: topic.clone(),
                                // Il QoS 1 in uscita non è ancora supportato (servirebbe un
                                // packet_id dedicato): consegniamo a QoS 0.
                                qos: QoS::AtMostOnce,
                                // MQTT-3.3.1-9: RETAIN=0 per sottoscrizioni già esistenti
                                // (i retained verranno consegnati al SUBSCRIBE in Fase 4).
                                retain: false,
                                // MQTT-3.3.1-2: DUP vale sulla singola tratta ed è sempre 0 a QoS 0.
                                dup: false,
                                packet_id: None,
                                payload: payload.clone(),
                            };
                            match tx.try_send(outgoing) {
                                Ok(()) => {}
                                Err(mpsc::error::TrySendError::Full(_)) => {
                                    warn!(
                                        "Dropping QoS 0 message for slow client [{}]: outbound queue full",
                                        target_client_id
                                    );
                                }
                                Err(mpsc::error::TrySendError::Closed(_)) => {}
                            }
                        }
                    }
                    Some(Ok(Packet::Disconnect)) | None => return Ok(()),
                    Some(Ok(Packet::Connect { .. })) => {
                        warn!(
                            "[{}] Received second CONNECT packet, protocol violation (MQTT-3.1.0-2), closing",
                            client_id
                        );
                        return Ok(());
                    }
                    Some(Ok(
                        packet @ (Packet::ConnAck { .. }
                        | Packet::SubAck { .. }
                        | Packet::UnsubAck { .. }
                        | Packet::PingResp),
                    )) => {
                        warn!(
                            "[{}] Received server-only packet {:?}, protocol violation, closing",
                            client_id, packet
                        );
                        return Ok(());
                    }
                    Some(Ok(Packet::PubAck { .. })) => {
                        // Fase 2: gestione PUBACK dal subscriber per QoS 1 in uscita.
                    }
                    Some(Err(e)) => {
                        warn!("Protocol error from [{}]: {:?}", client_id, e);
                        return Ok(());
                    }
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
        let filters = HashSet::from(["new/topic".to_string()]);
        state.release_session("client", 1, &filters).await;

        assert!(state.sessions.read().await.contains_key("client"));
        assert_eq!(state.trie.read().await.match_topic("new/topic").len(), 1);

        // La proprietaria legittima (conn 2) fa pulizia davvero
        state.release_session("client", 2, &filters).await;
        assert!(!state.sessions.read().await.contains_key("client"));
        assert!(state.trie.read().await.match_topic("new/topic").is_empty());
    }

    #[test]
    fn test_apply_subscribe_mixed_valid_and_invalid() {
        let mut trie = TopicTrie::new();
        let mut filters = HashSet::new();
        let topics = vec![
            SubscribeTopic {
                filter: "a/#/b".into(),
                qos: QoS::AtMostOnce,
            },
            SubscribeTopic {
                filter: "ok/+".into(),
                qos: QoS::AtLeastOnce,
            },
            SubscribeTopic {
                filter: "a+".into(),
                qos: QoS::AtMostOnce,
            },
        ];

        let codes = apply_subscribe(&mut trie, "c1", &topics, &mut filters);
        assert_eq!(
            codes,
            vec![
                SubAckReturnCode::Failure,
                SubAckReturnCode::from(granted_qos(QoS::AtLeastOnce)),
                SubAckReturnCode::Failure,
            ]
        );
        assert_eq!(filters.len(), 1);
        assert!(filters.contains("ok/+"));
        assert_eq!(trie.match_topic("ok/temp").len(), 1);
        assert!(trie.match_topic("a/x/b").is_empty());
        assert!(trie.match_topic("a+").is_empty());
    }

    #[test]
    fn test_apply_subscribe_deduplicates_filters() {
        let mut trie = TopicTrie::new();
        let mut filters = HashSet::new();
        let topics = vec![SubscribeTopic {
            filter: "sensors/+/temp".into(),
            qos: QoS::AtMostOnce,
        }];

        for _ in 0..1000 {
            let codes = apply_subscribe(&mut trie, "c1", &topics, &mut filters);
            assert_eq!(codes, vec![SubAckReturnCode::SuccessQoS0]);
        }

        assert_eq!(filters.len(), 1);
        assert!(filters.contains("sensors/+/temp"));
    }

    #[tokio::test]
    async fn test_send_with_timeout_errors_when_peer_does_not_read() {
        // Buffer minuscolo (16 byte) e nessun lettore su `_peer`.
        let (writer, _peer) = tokio::io::duplex(16);
        let mut framed = Framed::new(writer, MqttCodec::default());

        // Pacchetto più grande del buffer: la scrittura si blocca finché scade il timeout.
        let large_packet = Packet::Publish {
            topic: "slow/consumer".into(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            payload: bytes::Bytes::from(vec![b'x'; 256]),
        };

        let result = send_with_timeout(&mut framed, large_packet, Duration::from_millis(50)).await;
        assert!(
            result.is_err(),
            "send_with_timeout doveva restituire errore di timeout"
        );
        let err = result.unwrap_err();
        let io_err = err
            .downcast_ref::<std::io::Error>()
            .expect("atteso std::io::Error");
        assert_eq!(io_err.kind(), std::io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn test_idle_socket_without_connect_is_closed_on_timeout() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = BrokerState::default();

        let server_task = tokio::spawn(async move {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            let config = BrokerConfig {
                connect_timeout: Duration::from_millis(100),
                ..Default::default()
            };
            let _ = handle_client(socket, peer_addr, state, config).await;
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, MqttCodec::default());

        // Non inviamo nulla: entro 1 secondo il broker deve chiudere la connessione.
        let res = timeout(Duration::from_secs(1), framed.next())
            .await
            .expect("il broker non ha chiuso la socket muta dopo CONNECT_TIMEOUT");
        assert!(res.is_none());
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_subscribe_if_owner_rejects_replaced_connection() {
        let state = BrokerState::default();
        let (first, _first_rx) = handle(1);
        let (second, _second_rx) = handle(2);
        state.register_session("c", first).await;
        state.register_session("c", second).await;

        let topics = vec![SubscribeTopic {
            filter: "x/y".into(),
            qos: QoS::AtMostOnce,
        }];
        let mut old_filters = HashSet::new();
        let res_old = subscribe_if_owner(&state, "c", 1, &topics, &mut old_filters).await;
        assert!(
            res_old.is_none(),
            "la connessione sostituita non deve poter iscriversi nel trie"
        );
        assert!(state.trie.read().await.match_topic("x/y").is_empty());
        assert!(old_filters.is_empty());

        let mut new_filters = HashSet::new();
        let res_new = subscribe_if_owner(&state, "c", 2, &topics, &mut new_filters).await;
        assert_eq!(res_new, Some(vec![SubAckReturnCode::SuccessQoS0]));
        assert_eq!(state.trie.read().await.match_topic("x/y").len(), 1);
        assert!(new_filters.contains("x/y"));
    }

    #[tokio::test]
    async fn test_unsubscribe_if_owner_removes_for_owner_and_ignores_replaced() {
        let state = BrokerState::default();
        let (first, _first_rx) = handle(1);
        let (second, _second_rx) = handle(2);
        state.register_session("c", first).await;
        state.register_session("c", second).await;

        let topics = vec![SubscribeTopic {
            filter: "x/y".into(),
            qos: QoS::AtMostOnce,
        }];
        let mut new_filters = HashSet::new();
        assert!(
            subscribe_if_owner(&state, "c", 2, &topics, &mut new_filters)
                .await
                .is_some()
        );
        assert_eq!(state.trie.read().await.match_topic("x/y").len(), 1);

        // Connessione sostituita (conn 1) riceve None e il trie non cambia.
        let mut old_filters = HashSet::from(["x/y".to_string()]);
        let res_old =
            unsubscribe_if_owner(&state, "c", 1, &["x/y".to_string()], &mut old_filters).await;
        assert!(res_old.is_none());
        assert_eq!(state.trie.read().await.match_topic("x/y").len(), 1);

        // Connessione proprietaria (conn 2) rimuove il filtro e il trie torna vuoto.
        let res_new =
            unsubscribe_if_owner(&state, "c", 2, &["x/y".to_string()], &mut new_filters).await;
        assert_eq!(res_new, Some(()));
        assert!(state.trie.read().await.match_topic("x/y").is_empty());
        assert!(new_filters.is_empty());
    }

    #[test]
    fn test_default_broker_config_matches_codec_default_max_packet_size() {
        use bytes::{BufMut, BytesMut};
        use ferrite_protocol::varint::encode_varint;
        use tokio_util::codec::Decoder;

        let default_max = BrokerConfig::default().max_packet_size;
        let mut codec = MqttCodec::default();

        // Esattamente default_max: accettato (Ok(None) perché manca il corpo).
        let mut exact = BytesMut::new();
        exact.put_u8(0x30);
        let mut varint = Vec::with_capacity(4);
        encode_varint(default_max, &mut varint).unwrap();
        exact.put_slice(&varint);
        assert_eq!(codec.decode(&mut exact).unwrap(), None);

        // default_max + 1: rifiutato da MqttCodec::default() con PacketTooLarge.
        let mut over = BytesMut::new();
        over.put_u8(0x30);
        varint.clear();
        encode_varint(default_max + 1, &mut varint).unwrap();
        over.put_slice(&varint);
        assert!(matches!(
            codec.decode(&mut over),
            Err(ProtocolError::PacketTooLarge)
        ));
    }

    #[tokio::test]
    async fn test_unsubscribe_with_invalid_filter_closes_connection_without_unsuback() {
        use ferrite_protocol::ProtocolVersion;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let state = BrokerState::default();

        let server_task = tokio::spawn(async move {
            let (socket, peer_addr) = listener.accept().await.unwrap();
            let _ = handle_client(socket, peer_addr, state, BrokerConfig::default()).await;
        });

        let stream = TcpStream::connect(addr).await.unwrap();
        let mut framed = Framed::new(stream, MqttCodec::default());

        framed
            .send(Packet::Connect {
                protocol_version: ProtocolVersion::Mqtt311,
                clean_session: true,
                keep_alive: 60,
                client_id: "unsub-invalid-client".into(),
                will: None,
                username: None,
                password: None,
            })
            .await
            .unwrap();
        let connack = timeout(Duration::from_secs(1), framed.next())
            .await
            .expect("timeout su CONNACK")
            .expect("EOF su CONNACK")
            .expect("errore codec su CONNACK");
        assert!(matches!(
            connack,
            Packet::ConnAck {
                return_code: ConnectReturnCode::Accepted,
                ..
            }
        ));

        framed
            .send(Packet::Subscribe {
                packet_id: 1,
                topics: vec![SubscribeTopic {
                    filter: "a/#".into(),
                    qos: QoS::AtMostOnce,
                }],
            })
            .await
            .unwrap();
        let suback = timeout(Duration::from_secs(1), framed.next())
            .await
            .expect("timeout su SUBACK")
            .expect("EOF su SUBACK")
            .expect("errore codec su SUBACK");
        assert!(matches!(suback, Packet::SubAck { packet_id: 1, .. }));

        framed
            .send(Packet::Unsubscribe {
                packet_id: 2,
                filters: vec!["valid/topic".into(), "a/#/b".into()],
            })
            .await
            .unwrap();

        let res = timeout(Duration::from_secs(1), framed.next())
            .await
            .expect("il broker non ha chiuso la connessione dopo UNSUBSCRIBE invalido");
        assert!(
            res.is_none(),
            "attesa chiusura connessione senza UNSUBACK, ricevuto: {:?}",
            res
        );

        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn test_collect_targets_holds_trie_read_lock_while_acquiring_sessions() {
        let state = BrokerState::default();
        let (session, _rx) = handle(1);
        state.register_session("c", session).await;
        state
            .trie
            .write()
            .await
            .subscribe("t", "c".into(), QoS::AtMostOnce);

        let sessions_guard = state.sessions.write().await;

        let state_clone = state.clone();
        let task = tokio::spawn(async move { collect_targets(&state_clone, "t").await });
        tokio::task::yield_now().await;

        assert!(
            state.trie.try_write().is_err(),
            "collect_targets deve mantenere il read lock sul trie mentre attende il lock su sessions"
        );

        drop(sessions_guard);
        let targets = task.await.unwrap();
        assert_eq!(targets.len(), 1);
        assert_eq!(targets[0].0, "c");
    }
}
