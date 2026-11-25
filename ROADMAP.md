# Implementation Plan: FerriteMQ (`ferrite-mq`)

Questo documento definisce la roadmap esecutiva e l'architettura dei moduli per lo sviluppo della suite MQTT `ferrite-mq` in Rust.

La metodologia di progettazione segue la catena di dipendenza logica:

```text
ferrite-protocol → ferrite-broker-core → ferrite-broker → ferrite-client → integration tests
```

*Ultimo aggiornamento: 2026-10-07. Stato dei test: 31 test passano (`cargo test --workspace`).*

---

## 1. Stato Attuale della Codebase (Baseline)

### `ferrite-protocol`
- [x] Variable Byte Integer encoding/decoding (`varint`).
- [x] Codec basato su `tokio_util::codec::{Decoder, Encoder}` con gestione zero-copy tramite `bytes::Bytes` e `BytesMut`.
- [x] Supporto pacchetti di controllo:
  - `CONNECT` (incluso il campo **Will**: topic, payload, QoS, retain), `CONNACK`
  - `PUBLISH` (header per QoS 0, 1, 2; l'handshake QoS 2 è in Fase 5)
  - `PUBACK` (Fixed header + packet ID a 2 byte)
  - `SUBSCRIBE`, `SUBACK`
  - `PINGREQ`, `PINGRESP`
  - `DISCONNECT`
- [x] I pacchetti riconosciuti ma non ancora implementati (`PUBREC`, `PUBREL`, `PUBCOMP`, `AUTH`) producono un errore esplicito `InvalidPacketType` invece di sparire in silenzio.
- [x] `QoS` ordinabile (`PartialOrd`/`Ord`) e `From<QoS> for SubAckReturnCode`.
- [x] `UNSUBSCRIBE` / `UNSUBACK` (encode/decode).

### `ferrite-broker-core`
- [x] `TopicTrie`: albero di instradamento gerarchico in memoria.
- [x] Supporto wildcard:
  - Single-level (`+`)
  - Multi-level (`#`)
- [x] Sottoscrizione (`subscribe`), rimozione per filtro (`unsubscribe`) e rimozione di tutte le sottoscrizioni di un client (`remove_client`).
- [x] Funzione di matching concorrente (`match_topic`).
- [x] Astrazione `run_server(TcpListener)` riutilizzabile sia dal binario che nei test di integrazione.
- [x] SUBACK con il QoS **realmente concesso** (`MAX_GRANTED_QOS`, oggi QoS 0 in uscita); il trie memorizza il QoS concesso.
- [x] Pulizia della sessione garantita a fine connessione, anche in caso di errore di I/O.
- [x] Un nuovo `CONNECT` con un `client_id` già connesso sostituisce la vecchia sessione e ne chiude la connessione (MQTT-3.1.4-2). Ogni connessione ha un `conn_id`; solo la proprietaria corrente del `client_id` può rimuoverne sessione e sottoscrizioni.
- [x] Lock sul trie rilasciato prima dell'I/O di rete. Ordine dei lock da rispettare: prima `trie`, poi `sessions`.

### `ferrite-broker`
- [x] Demone eseguibile standalone su porta TCP `1883`.
- [x] Inizializzazione logging asincrono strutturato (`tracing`, `tracing-subscriber`).
- [x] Gestione concorrenza per connessione con task dedicati `tokio::spawn`.
- [x] Routing concorrente con `Arc<RwLock<TopicTrie>>` e canali `tokio::sync::mpsc`.
- [x] Watchdog di inattività con timeout pari a 1.5 × `keep_alive`. Viene riarmato **solo** dai pacchetti ricevuti dal client (il traffico in uscita non conta); `keep_alive = 0` lo disabilita.
- [x] Gestione QoS 1 server-side verso il publisher: invio automatico di `Packet::PubAck { packet_id }` alla ricezione di `PUBLISH` con `QoS::AtLeastOnce`.
- [ ] Consegna QoS 1 **verso i subscriber** (oggi sempre QoS 0): Fase 2.

### `ferrite-client`
- [x] Driver asincrono separato basato su pattern Driver-Handle (`AsyncClient` + background task).
- [x] Handshake di connessione `CONNECT` → `CONNACK`.
- [x] Pubblicazione a **QoS 0** via `publish(topic, payload)` (Fire & Forget).
- [x] Pubblicazione a **QoS 1** via `publish_qos1(topic, payload)`: la chiamata si sblocca solo alla ricezione del `PUBACK` (pattern `oneshot` + tabella `pending_acks`).
- [x] Packet ID in [1, 65535] generati con `fetch_update` (atomico, mai 0), condivisi da publish e subscribe.
- [x] Registrazione sottoscrizioni via `subscribe(filter, qos)`.
- [x] Disconnessione pulita via `disconnect()`.
- [x] Heartbeat periodico automatico con `tokio::time::interval` impostato sul 75% del keep-alive negoziato (con soglia minima per prevenire panic).
- [ ] Risposta automatica `PUBACK` ai `PUBLISH` QoS 1 ricevuti: Fase 2.
- [ ] Impostare il Will tramite `MqttOptions`: Fase 4.

### Workspace
- [x] `ferrite-cli` eredita i metadati del workspace (edition 2021).
- [x] `Cargo.lock` versionato (il workspace contiene binari).
- [x] File di licenza `LICENSE-MIT` e `LICENSE-APACHE` compilati.
- [x] `README.md` iniziale.
- [ ] `ferrite-cli`: ancora un placeholder.
- [ ] CI (GitHub Actions: `cargo fmt --check`, `cargo clippy`, `cargo test`).
- [ ] Il codice non è ancora formattato con `cargo fmt` (diff pre-esistente).

---

## 2. Tabella di Marcia (Milestones)

| Fase | Crate Coinvolti | Obiettivo Primario | Stato |
| :--- | :--- | :--- | :--- |
| **Fase 1** | `ferrite-client`, `tests` | QoS 1 Publisher con pattern `oneshot` | **Completata** |
| **Fase 1.5** | Tutti i crate | Correzioni post-revisione (packet ID, SUBACK, codec, sessioni, watchdog) | **Completata** |
| **Fase 2** | `ferrite-broker-core`, `ferrite-client` | Flusso QoS 1 verso Subscriber (tracking ACK di ritorno) | **Prossima (priorità alta)** |
| **Fase 3** | `ferrite-broker-core` | Sessioni persistenti (`CleanSession = false`) e code messaggi offline | Media |
| **Fase 4** | `ferrite-broker-core`, `ferrite-protocol` | Retained messages e Last Will and Testament (il lato protocollo del Will è già fatto) | Media |
| **Fase 5** | Tutti i crate | QoS 2 completo (`PUBREC`, `PUBREL`, `PUBCOMP`) | Media |
| **Fase 6** | `ferrite-broker`, `ferrite-client` | Sicurezza: TLS (`tokio-rustls`), autenticazione e ACL | Bassa |
| **Fase 7** | `ferrite-broker-core` | Ottimizzazioni concorrenza: sharded trie e lock contention | Bassa |

---

## 3. Specifiche di Dettaglio per Fase

### Fase 1: QoS 1 nel Client (`ferrite-client`) — completata

Il client può pubblicare a QoS 1 bloccando l'invocazione asincrona finché il broker non restituisce il relativo pacchetto `PUBACK`.

- `ClientError::AckChannelDropped` per il caso in cui il driver termini prima di consegnare l'ACK.
- `ClientCommand` suddiviso in `PublishQoS0 { topic, payload }` e `PublishQoS1 { packet_id, topic, payload, ack_sender }`.
- Nel driver, `pending_acks: HashMap<u16, oneshot::Sender<()>>`: alla ricezione di `PUBACK` si rimuove l'entry e si invoca `ack_sender.send(())`.
- API pubblica: `publish_qos1<T: Into<String>, P: Into<Bytes>>(&self, topic, payload) -> Result<(), ClientError>`.
- Test di integrazione: `test_client_publish_qos1_with_ack`.

### Fase 1.5: Correzioni post-revisione — completata

| # | Problema | Soluzione |
| :--- | :--- | :--- |
| 1-2 | `next_packet_id` con race e possibile ID 0; il driver usava un contatore senza controllo per `SUBSCRIBE` | `next_id(&AtomicU16)` con `fetch_update`, usata ovunque |
| 3 | SUBACK con return code scritto a mano, trie con QoS richiesto | `MAX_GRANTED_QOS` + `granted_qos()`; stesso valore nel trie e nel SUBACK |
| 4 | Pacchetti non supportati ignorati con `Ok(None)` dopo aver consumato i byte | Errore esplicito; `match` esaustivo senza `_` |
| 5 | Will non rappresentabile in `Packet::Connect` | Struct `Will` e campo `will: Option<Will>`, encode/decode dei flag e del payload |
| 6 | Test del codec fuori da `#[cfg(test)]` | Racchiusi in `mod tests` |
| 7 | `?` in `handle_client` saltava la pulizia | Il loop vive in `session_loop`; `release_session` viene eseguita sempre dopo |
| 8 | Due connessioni con lo stesso `client_id` | `conn_id` + `register_session`/`release_session`; la vecchia connessione viene chiusa |
| 9 | Write lock sul trie tenuto durante `send(SubAck)` | Guard confinato in un blocco che termina prima dell'I/O |
| 10 | `ferrite-cli` con edition 2024 e senza metadati del workspace | Ereditati dal workspace |
| 11 | `Cargo.lock` ignorato | Rimosso da `.gitignore` |
| 12 | `README` e licenze vuoti | Compilati |
| 14 | Il traffico in uscita azzerava il watchdog | `Sleep` fissato fuori dal loop e riarmato solo dai pacchetti in ingresso |

Punto 13 (`ferrite-broker-core` senza un tipo di errore proprio): non è un difetto; resta un miglioramento di design da valutare quando la libreria avrà un'API pubblica stabile. Oggi `run_server` restituisce `Box<dyn Error + Send + Sync>`.

### Fase 2: Ciclo Completo QoS 1 verso i Subscriber

Attualmente il broker recapita i messaggi ai subscriber con `packet_id: None` (comportamento QoS 0) e il SUBACK lo dichiara correttamente tramite `MAX_GRANTED_QOS = QoS::AtMostOnce`.

- **Broker Core**:
  - Portare `MAX_GRANTED_QOS` a `QoS::AtLeastOnce`.
  - Calcolare il QoS effettivo di consegna come `min(QoS del publish, QoS concesso al subscriber)`.
  - Quando il QoS effettivo è 1, generare un `packet_id` per quella specifica connessione (stesso schema di `next_id`).
  - Mantenere `in_flight_messages: HashMap<u16, Message>` per ogni sessione client.
  - Gestire il `PUBACK` proveniente dal subscriber e rimuovere il messaggio dalla tabella dei messaggi in volo (oggi cade nel ramo `Some(Ok(_)) => {}`).
- **Client**:
  - Nel loop di ricezione di `ferrite-client`, intercettare i `Packet::Publish` con `qos == QoS::AtLeastOnce` e rispondere automaticamente con `Packet::PubAck { packet_id }`.
  - Gestire la collisione di un packet ID con una voce ancora presente in `pending_acks` (dopo un wrap-around a 65535).
- **Test**: un subscriber QoS 1 riceve il `PUBLISH` con `packet_id` e il broker rimuove il messaggio da `in_flight_messages` solo dopo il `PUBACK`.

### Fase 3: Sessioni Persistenti (`CleanSession = false`)

- **Persistenza in memoria**: tabella globale `sessions: HashMap<String, SessionState>` dove `SessionState` sopravvive alla disconnessione se `clean_session == false`. Partire dall'attuale `SessionHandle { conn_id, tx }`.
- **Coda offline**: bufferizzare i messaggi QoS 1 destinati a subscriber disconnessi.
- **Handshake `CONNECT`**: se `clean_session == false` e la sessione esiste, riassociare la nuova socket alla sessione, svuotare la coda messaggi e restituire `session_present = true` nel `CONNACK`.
- **Takeover**: oggi `register_session` scarta le sottoscrizioni della vecchia sessione (solo clean session). Con sessioni persistenti vanno invece mantenute; spostare le sottoscrizioni nello stato per-sessione chiude anche la finestra di race descritta in "Problemi noti".

### Fase 4: Retained Messages & Last Will and Testament (LWT)

- **Retained Messages**:
  - Mantenere `retained_messages: HashMap<String, Packet>` per l'ultimo messaggio con `retain = true`.
  - Al `SUBSCRIBE`, verificare se il nuovo filtro intercetta messaggi conservati e recapitarli subito.
  - Cancellare il messaggio conservato se giunge un payload vuoto con `retain = true`.
- **Last Will and Testament**:
  - ✅ Parsing e serializzazione dei campi Will (`topic`, `payload`, `qos`, `retain`) nel `CONNECT`.
  - Esporre il Will in `MqttOptions` lato client.
  - Conservare il Will nella sessione del broker.
  - Se la connessione cade per errore di socket o inattività (senza `DISCONNECT`), o se viene sostituita da un takeover, pubblicare il messaggio Will ai subscriber. Il Will va scartato quando arriva un `DISCONNECT` pulito.

### Fase 5: QoS 2 (Exactly Once Delivery)

- **Protocollo**: serializzazione e deserializzazione di `PUBREC` (tipo 5), `PUBREL` (tipo 6), `PUBCOMP` (tipo 7). Oggi questi tipi restituiscono `InvalidPacketType`.
- **Macchina a stati**:
  - Passo 1: `PUBLISH (QoS 2)` → il broker memorizza il packet ID e risponde con `PUBREC`.
  - Passo 2: il client risponde con `PUBREL` → il broker inoltra il messaggio al `TopicTrie` e conclude con `PUBCOMP`.
  - Protezione contro messaggi duplicati durante le ritrasmissioni.

### Fase 6: Sicurezza, TLS e Controllo Accessi

- **Cifratura**: integrazione di `tokio-rustls` per l'ascolto sulla porta standard TLS `8883`.
- **Autenticazione**: trait asincrono `Authenticator` per la verifica di username e password trasmessi in fase di `CONNECT`.
- **ACL**: validazione dei permessi di `Subscribe` e `Publish` in base all'identità del client e ai pattern di topic.

### Fase 7: Concorrenza Avanzata e Scalabilità

- **Deadlock da backpressure** (vedi "Problemi noti"): non attendere `tx.send(..).await` tenendo il read lock su `sessions`; usare `try_send` con una politica di scarto o disconnessione dei consumer lenti.
- **Sharded Trie**: suddivisione del `TopicTrie` in partizioni indicizzate tramite hashing del root topic per minimizzare la contesa del lock `RwLock`.
- **Tabella sessioni lock-free**: collezioni concorrenti (es. `DashMap`) al posto di `RwLock<HashMap<...>>`.
- **Potatura del trie**: `remove_recursive` calcola già se un ramo è vuoto ma non lo elimina.
- **Suite di benchmark**: crate `ferrite-benchmarks` con `criterion` per misurare il throughput del codec e la latenza del matching dei topic con carichi massivi.

---

## 4. Problemi noti

| Problema | Dove | Quando risolverlo |
| :--- | :--- | :--- |
| ~~Possibile deadlock: nel ramo `Publish` si fa `tx.send(..).await` su un canale limitato tenendo il read lock su `sessions`. Se due client si inviano messaggi e le code si riempiono, restano in attesa reciproca.~~ | `broker-core/server.rs` | Risolto (issue #3: fan-out non bloccante con `try_send` e timeout di scrittura) |
| ~~Finestra di race stretta: se un takeover avviene mentre la vecchia connessione elabora un `SUBSCRIBE`, può reiscrivere un filtro per il `client_id` ormai appartenente alla nuova.~~ | `broker-core/server.rs` | Risolto (issue #12: `biased;` nel `select!` + controllo proprietà sotto lock in `subscribe_if_owner`) |
| ~~`encode_string` tronca con `as u16` le stringhe più lunghe di 65535 byte invece di restituire un errore.~~ | `protocol/codec.rs` | Risolto (issue #10: `encode_str`/`encode_bytes` con `StringTooLong` e scrittura atomica) |
| Il `Will` ricevuto nel `CONNECT` è decodificato ma il broker lo ignora. | `broker-core/server.rs` | Fase 4 |
| ~~Il broker non gestisce `UNSUBSCRIBE`: il trie ha `unsubscribe`, ma non è raggiungibile dalla rete.~~ | `protocol`, `broker-core` | Risolto (issue #9: `UNSUBSCRIBE`/`UNSUBACK` end-to-end in protocollo, broker e client) |


---

## 5. Piano di lavoro (revisione del 2026-11-25)

Il tracciamento è su GitHub Issues e Milestones: https://github.com/domenico-cangelli/ferrite-mq/issues
Ramo di integrazione: `develop`. Ogni correzione ha un ramo `fix/<issue>-<nome>`, una PR con `Closes #N` e almeno un test.

Correzioni alla sezione 4: il deadlock (#3) è più grave di come descritto (un solo client può bloccarsi da solo e congelare il broker, perché il lock di tokio è write-fair). La race del takeover (#12) non è stretta: la perdita è permanente. `encode_string` (#10) tronca anche payload del Will e password. Il filtro di log predefinito nasconde i log di `ferrite_broker_core` (#11).

Ordine di esecuzione (priorità e dipendenze):

| Ordine | Issue | Milestone | Dipende da |
| :--- | :--- | :--- | :--- |
| 1 | #2 fmt e warning clippy | M0 Qualità e CI | - |
| 2 | #1 CI GitHub Actions | M0 | #2 |
| 3 | #3 deadlock back-pressure | M1 Robustezza broker | #1 |
| 4 | #4 trie: limiti, validazione, potatura | M1 | #1 |
| 5 | #5 SUBSCRIBE/PUBLISH validati | M1 | #4 |
| 6 | #12 race takeover | M1 | #3 |
| 7 | #11 limiti memoria e config | M1 | #1 |
| 8 | #6 CONNECT conforme | M2 Conformità | #1 |
| 9 | #7 RETAIN/DUP nell'inoltro | M2 | #1 |
| 10 | #9 UNSUBSCRIBE end-to-end | M2 | #4 |
| 11 | #10 codec validazione rigorosa | M2 | #1 |
| 12 | #8 QoS 2 rifiutato in modo pulito | M2 | #1 |
| 13 | #13 client: PINGRESP e canale incoming | M3 Client robusto | #1 |
| 14 | #14 client: chiusura, timeout, SUBACK | M3 | #13 |
| 15 | #15 Fase 2: QoS 1 verso subscriber | M4 | #3, #13 |
| 16 | #16 Fase 3: sessioni persistenti | M5 | #12, #15 |
| 17 | #17 Fase 4: retained e Will | M6 | #16 |
| 18 | #18 Fase 5: QoS 2 | M7 | #15, #8 |
| 19 | #19 Fase 6: TLS, auth, ACL | M8 | #6 |
| 20 | #20 Fase 7: benchmark e scalabilità | M9 | #15 |
| 21 | #21 ferrite-cli | M10 | #14 |
| 22 | #22 documentazione | M0 | - |
| 23 | #23 conformità e fuzzing | M10 | #10 |
| 24 | #24 idee oltre Paho e Mosquitto | M10 | - |

Idee per superare i limiti di Paho e Mosquitto: vedi #24 (broker multi-thread, client async con backpressure, MQTT 5, metriche, persistenza su disco, cluster).

I passi piccoli (file, risultato atteso, comando di verifica) sono nel corpo di ogni issue.
