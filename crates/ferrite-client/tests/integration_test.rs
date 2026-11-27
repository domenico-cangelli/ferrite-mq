use bytes::Bytes;
use ferrite_broker_core::run_server;
use ferrite_client::{connect, MqttOptions, Packet, QoS};
use ferrite_protocol::{MqttCodec, ProtocolVersion};
use futures::{SinkExt, StreamExt};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::codec::Framed;

/// Avvia il broker in background e restituisce l'indirizzo dinamico (es. 127.0.0.1:45321)
async fn start_test_broker() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = run_server(listener).await;
    });

    addr
}

#[tokio::test]
async fn test_pub_sub_roundtrip() {
    // 1. Avvia il broker embedded
    let broker_addr = start_test_broker().await;

    // 2. Connetti il Client Subscriber
    let sub_opts = MqttOptions::new("subscriber-1", broker_addr);
    let (sub_client, mut sub_rx) = connect(sub_opts).await.expect("Failed to connect sub");

    // Iscriviti al topic (subscribe attende il SUBACK dal broker)
    let granted = sub_client
        .subscribe("factory/sensors/temperature", QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");
    assert_eq!(granted, QoS::AtMostOnce);

    // 4. Connetti il Client Publisher
    let pub_opts = MqttOptions::new("publisher-1", broker_addr);
    let (pub_client, _pub_rx) = connect(pub_opts).await.expect("Failed to connect pub");

    // 5. Invia il messaggio
    let expected_payload = Bytes::from_static(b"42.5");
    pub_client
        .publish("factory/sensors/temperature", expected_payload.clone())
        .await
        .expect("Failed to publish");

    // 6. Verifica che il subscriber riceva il pacchetto PUBLISH con il payload corretto
    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("Timeout waiting for PUBLISH message")
        .expect("Channel closed");

    match received {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "factory/sensors/temperature");
            assert_eq!(payload, expected_payload);
        }
        other => panic!("Expected Publish packet, got {:?}", other),
    }

    // 7. Cleanup
    sub_client.disconnect().await.unwrap();
    pub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_automatic_pingreq() {
    let broker_addr = start_test_broker().await;

    // Impostiamo un keep_alive brevissimo (1 secondo -> watchdog broker = 1.5s)
    let mut opts = MqttOptions::new("ping-client", broker_addr);
    opts.keep_alive = 1;

    let (client, _incoming_rx) = connect(opts).await.expect("Connection failed");

    // Attendiamo 2.2s (> 1.5s di watchdog): senza PINGREQ automatici il broker avrebbe chiuso.
    tokio::time::sleep(Duration::from_millis(2200)).await;

    timeout(
        Duration::from_secs(2),
        client.publish_qos1("ping/alive", Bytes::from_static(b"ok")),
    )
    .await
    .expect("Timeout waiting for PUBACK: automatic PINGREQ failed to keep connection alive")
    .expect("publish_qos1 failed");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_broker_disconnects_inactive_client() {
    let broker_addr = start_test_broker().await;

    // Connettiamoci con uno stream TCP grezzo senza usare il client automatico,
    // così possiamo simulare un client inerte che non invia ping.
    let stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    let mut framed = Framed::new(stream, MqttCodec::default());

    // Invia CONNECT con keep_alive = 1 secondo (timeout broker = 1.5s)
    let connect = Packet::Connect {
        protocol_version: ProtocolVersion::Mqtt311,
        clean_session: true,
        keep_alive: 1,
        client_id: "lazy-client".into(),
        will: None,
        username: None,
        password: None,
    };
    framed.send(connect).await.unwrap();

    // Ricevi CONNACK
    let connack = framed.next().await.unwrap().unwrap();
    assert!(matches!(connack, Packet::ConnAck { .. }));

    // Rimaniamo in silenzio: entro ~2 secondi il broker deve chiudere il socket (restituire None)
    let disconnected = timeout(Duration::from_secs(3), framed.next()).await;

    match disconnected {
        Ok(None) => {
            // Successo: la connessione è stata chiusa dal broker per timeout di inattività
        }
        Ok(Some(packet)) => panic!("Ricevuto pacchetto inaspettato: {:?}", packet),
        Err(_) => {
            panic!("Il broker non ha disconnesso il client entro la finestra temporale prevista")
        }
    }
}

#[tokio::test]
async fn test_broker_puback_response_for_qos1() {
    let broker_addr = start_test_broker().await;

    // Connessione diretta via socket raw per verificare il frame di risposta del broker
    let stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    let mut framed = Framed::new(stream, MqttCodec::default());

    // 1. Handshake CONNECT
    framed
        .send(Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt311,
            clean_session: true,
            keep_alive: 60,
            client_id: "qos1-tester".into(),
            will: None,
            username: None,
            password: None,
        })
        .await
        .unwrap();

    let connack = framed.next().await.unwrap().unwrap();
    assert!(matches!(connack, Packet::ConnAck { .. }));

    // 2. Invio di un PUBLISH a QoS 1 con packet_id = 42
    framed
        .send(Packet::Publish {
            topic: "telemetry/vibration".into(),
            qos: QoS::AtLeastOnce,
            retain: false,
            dup: false,
            packet_id: Some(42),
            payload: Bytes::from_static(b"critical_vibration_alert"),
        })
        .await
        .unwrap();

    // 3. Verifica ricezione del PUBACK corrispondente
    let response = timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("Timeout waiting for PUBACK")
        .expect("Connection closed prematurely")
        .expect("Protocol decode error");

    assert_eq!(response, Packet::PubAck { packet_id: 42 });
}

#[tokio::test]
async fn test_client_publish_qos1_with_ack() {
    let broker_addr = start_test_broker().await;

    // 1. Connetti subscriber
    let sub_opts = MqttOptions::new("sub-qos1", broker_addr);
    let (sub_client, mut sub_rx) = connect(sub_opts).await.unwrap();

    sub_client
        .subscribe("orders/processed", QoS::AtLeastOnce)
        .await
        .unwrap();

    // 2. Connetti publisher
    let pub_opts = MqttOptions::new("pub-qos1", broker_addr);
    let (pub_client, _pub_rx) = connect(pub_opts).await.unwrap();

    // 3. Pubblica con publish_qos1: la chiamata completa solo se il broker invia PUBACK
    let payload = Bytes::from_static(b"order_id_8941");
    let pub_result = timeout(
        Duration::from_secs(2),
        pub_client.publish_qos1("orders/processed", payload.clone()),
    )
    .await;

    assert!(
        pub_result.is_ok(),
        "publish_qos1 timed out waiting for PUBACK"
    );
    pub_result.unwrap().expect("publish_qos1 failed");

    // 4. Verifica che il subscriber riceva il payload
    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match received {
        Packet::Publish {
            topic, payload: p, ..
        } => {
            assert_eq!(topic, "orders/processed");
            assert_eq!(p, payload);
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    sub_client.disconnect().await.unwrap();
    pub_client.disconnect().await.unwrap();
}
#[tokio::test]
async fn test_broker_suback_reports_granted_qos() {
    let broker_addr = start_test_broker().await;

    let stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    let mut framed = Framed::new(stream, MqttCodec::default());

    framed
        .send(Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt311,
            clean_session: true,
            keep_alive: 60,
            client_id: "suback-tester".into(),
            will: None,
            username: None,
            password: None,
        })
        .await
        .unwrap();
    assert!(matches!(
        framed.next().await.unwrap().unwrap(),
        Packet::ConnAck { .. }
    ));

    framed
        .send(Packet::Subscribe {
            packet_id: 7,
            topics: vec![
                ferrite_protocol::SubscribeTopic {
                    filter: "a/b".into(),
                    qos: QoS::AtMostOnce,
                },
                ferrite_protocol::SubscribeTopic {
                    filter: "c/d".into(),
                    qos: QoS::ExactlyOnce,
                },
            ],
        })
        .await
        .unwrap();

    let response = timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("Timeout waiting for SUBACK")
        .unwrap()
        .unwrap();

    match response {
        Packet::SubAck {
            packet_id,
            return_codes,
        } => {
            assert_eq!(packet_id, 7);
            // Un return code per ogni filtro, mai oltre quanto richiesto.
            assert_eq!(return_codes.len(), 2);
            assert_eq!(
                return_codes[0],
                ferrite_protocol::SubAckReturnCode::SuccessQoS0
            );
            assert_ne!(return_codes[1], ferrite_protocol::SubAckReturnCode::Failure);
        }
        other => panic!("Expected SubAck, got {:?}", other),
    }
}

/// Apre una connessione raw verso il broker ed esegue l'handshake CONNECT/CONNACK.
async fn raw_connect(
    broker_addr: SocketAddr,
    client_id: &str,
    keep_alive: u16,
) -> Framed<tokio::net::TcpStream, MqttCodec> {
    let stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    let mut framed = Framed::new(stream, MqttCodec::default());

    framed
        .send(Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt311,
            clean_session: true,
            keep_alive,
            client_id: client_id.into(),
            will: None,
            username: None,
            password: None,
        })
        .await
        .unwrap();

    let connack = framed.next().await.unwrap().unwrap();
    assert!(matches!(connack, Packet::ConnAck { .. }));
    framed
}

#[tokio::test]
async fn test_outgoing_traffic_does_not_reset_inactivity_watchdog() {
    let broker_addr = start_test_broker().await;

    // Subscriber "muto": keep_alive = 1s (timeout broker = 1.5s), dopo la SUBSCRIBE non invia più nulla.
    let mut silent = raw_connect(broker_addr, "silent-subscriber", 1).await;
    silent
        .send(Packet::Subscribe {
            packet_id: 1,
            topics: vec![ferrite_protocol::SubscribeTopic {
                filter: "noise".into(),
                qos: QoS::AtMostOnce,
            }],
        })
        .await
        .unwrap();
    assert!(matches!(
        silent.next().await.unwrap().unwrap(),
        Packet::SubAck { .. }
    ));

    // Un publisher invia un messaggio ogni 200ms per ~4s: il traffico *in uscita* verso
    // il subscriber non deve mantenerlo in vita.
    let (pub_client, _pub_rx) = connect(MqttOptions::new("noise-publisher", broker_addr))
        .await
        .unwrap();
    let publisher = tokio::spawn(async move {
        for _ in 0..20 {
            let _ = pub_client.publish("noise", Bytes::from_static(b"x")).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    });

    let started = std::time::Instant::now();
    let closed = timeout(Duration::from_secs(3), async {
        while silent.next().await.is_some() {}
    })
    .await;
    publisher.abort();

    assert!(
        closed.is_ok(),
        "il broker non ha disconnesso il client silenzioso: il traffico in uscita ha azzerato il watchdog"
    );
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn test_duplicate_client_id_disconnects_old_connection() {
    let broker_addr = start_test_broker().await;

    // Vecchia connessione, iscritta a "old/topic"
    let mut old = raw_connect(broker_addr, "dup-client", 60).await;
    old.send(Packet::Subscribe {
        packet_id: 1,
        topics: vec![ferrite_protocol::SubscribeTopic {
            filter: "old/topic".into(),
            qos: QoS::AtMostOnce,
        }],
    })
    .await
    .unwrap();
    assert!(matches!(
        old.next().await.unwrap().unwrap(),
        Packet::SubAck { .. }
    ));

    // Nuova connessione con lo stesso client_id
    let (new_client, mut new_rx) = connect(MqttOptions::new("dup-client", broker_addr))
        .await
        .unwrap();

    // MQTT-3.1.4-2: il broker deve chiudere la connessione precedente
    let closed = timeout(Duration::from_secs(2), old.next())
        .await
        .expect("il broker non ha chiuso la vecchia connessione con lo stesso client_id");
    assert!(closed.is_none());

    // La nuova connessione deve funzionare, anche dopo la pulizia della vecchia
    new_client
        .subscribe("new/topic", QoS::AtMostOnce)
        .await
        .unwrap();

    let (pub_client, _pub_rx) = connect(MqttOptions::new("dup-publisher", broker_addr))
        .await
        .unwrap();
    pub_client
        .publish("old/topic", Bytes::from_static(b"stale"))
        .await
        .unwrap();
    pub_client
        .publish("new/topic", Bytes::from_static(b"fresh"))
        .await
        .unwrap();

    // Il nuovo client riceve solo "new/topic": le vecchie sottoscrizioni sono state scartate
    let received = timeout(Duration::from_secs(2), new_rx.recv())
        .await
        .unwrap()
        .unwrap();
    match received {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "new/topic");
            assert_eq!(payload, Bytes::from_static(b"fresh"));
        }
        other => panic!("Expected Publish, got {:?}", other),
    }
}

#[tokio::test]
async fn test_self_subscriber_flood_does_not_freeze_broker() {
    let broker_addr = start_test_broker().await;

    // Client A: socket TCP grezzo, CONNECT + SUBSCRIBE "#"
    let mut client_a = raw_connect(broker_addr, "flood-client-a", 60).await;
    client_a
        .send(Packet::Subscribe {
            packet_id: 1,
            topics: vec![ferrite_protocol::SubscribeTopic {
                filter: "#".into(),
                qos: QoS::AtMostOnce,
            }],
        })
        .await
        .unwrap();
    assert!(matches!(
        client_a.next().await.unwrap().unwrap(),
        Packet::SubAck { .. }
    ));

    // Client A invia 500 PUBLISH QoS 0 senza mai leggere dal socket.
    // Lo eseguiamo in un task separato così anche se il buffer TCP di A si riempie
    // il test prosegue e verifica che il broker risponda agli altri client.
    let flood_payload = Bytes::from(vec![b'a'; 2048]);
    let flood_task = tokio::spawn(async move {
        for i in 0..500u32 {
            let packet = Packet::Publish {
                topic: format!("flood/{i}"),
                qos: QoS::AtMostOnce,
                retain: false,
                dup: false,
                packet_id: None,
                payload: flood_payload.clone(),
            };
            if client_a.feed(packet).await.is_err() {
                break;
            }
        }
        let _ = client_a.flush().await;
        // Manteniamo il socket aperto senza leggere finché il task non viene abortito.
        std::future::pending::<()>().await;
    });

    // Diamo al flood il tempo di saturare il canale interno di A.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Client B e C devono riuscire a connettersi, iscriversi e scambiare messaggi entro 3 secondi.
    let healthy_flow = timeout(Duration::from_secs(3), async {
        let (client_c, mut rx_c) = connect(MqttOptions::new("healthy-client-c", broker_addr))
            .await
            .expect("client C failed to connect");
        client_c
            .subscribe("health/check", QoS::AtMostOnce)
            .await
            .expect("client C failed to subscribe");

        let (client_b, _rx_b) = connect(MqttOptions::new("healthy-client-b", broker_addr))
            .await
            .expect("client B failed to connect");
        client_b
            .publish("health/check", Bytes::from_static(b"ok"))
            .await
            .expect("client B failed to publish");

        let received = rx_c.recv().await.expect("client C channel closed");
        match received {
            Packet::Publish { topic, payload, .. } => {
                assert_eq!(topic, "health/check");
                assert_eq!(payload, Bytes::from_static(b"ok"));
            }
            other => panic!("Expected Publish, got {:?}", other),
        }

        client_b.disconnect().await.unwrap();
        client_c.disconnect().await.unwrap();
    })
    .await;

    flood_task.abort();
    assert!(
        healthy_flow.is_ok(),
        "il broker si è congelato durante il flood del self-subscriber"
    );
}

#[tokio::test]
async fn test_subscribe_mixed_filters_returns_failure_and_keeps_connection_open() {
    let broker_addr = start_test_broker().await;

    let mut sub = raw_connect(broker_addr, "sub-mixed", 60).await;
    sub.send(Packet::Subscribe {
        packet_id: 10,
        topics: vec![
            ferrite_protocol::SubscribeTopic {
                filter: "a/#/b".into(),
                qos: QoS::AtMostOnce,
            },
            ferrite_protocol::SubscribeTopic {
                filter: "ok/+".into(),
                qos: QoS::AtLeastOnce,
            },
        ],
    })
    .await
    .unwrap();

    let response = timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("Timeout waiting for SUBACK")
        .expect("Connection closed after invalid filter")
        .expect("Protocol error");

    assert_eq!(
        response,
        Packet::SubAck {
            packet_id: 10,
            return_codes: vec![
                ferrite_protocol::SubAckReturnCode::Failure,
                ferrite_protocol::SubAckReturnCode::SuccessQoS0,
            ],
        }
    );

    // La connessione deve restare aperta e continuare a ricevere messaggi su "ok/+".
    let (pub_client, _pub_rx) = connect(MqttOptions::new("pub-mixed", broker_addr))
        .await
        .unwrap();
    pub_client
        .publish("ok/hello", Bytes::from_static(b"alive"))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("Timeout waiting for PUBLISH on open connection")
        .expect("Connection closed unexpectedly")
        .expect("Protocol error");
    match received {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "ok/hello");
            assert_eq!(payload, Bytes::from_static(b"alive"));
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    pub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_publish_invalid_topic_closes_connection_and_does_not_deliver() {
    let broker_addr = start_test_broker().await;

    let (sub_client, mut sub_rx) = connect(MqttOptions::new("sub-ab", broker_addr))
        .await
        .unwrap();
    sub_client.subscribe("a/b", QoS::AtMostOnce).await.unwrap();

    // 1. PUBLISH con wildcard "a/+" deve chiudere la connessione del publisher.
    let mut pub_wildcard = raw_connect(broker_addr, "pub-wildcard", 60).await;
    pub_wildcard
        .send(Packet::Publish {
            topic: "a/+".into(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            payload: Bytes::from_static(b"bad"),
        })
        .await
        .unwrap();
    let closed_wildcard = timeout(Duration::from_secs(2), pub_wildcard.next())
        .await
        .expect("il broker non ha chiuso la connessione dopo PUBLISH su 'a/+'");
    assert!(closed_wildcard.is_none());

    // 2. PUBLISH con topic vuoto "" deve chiudere la connessione del publisher.
    let mut pub_empty = raw_connect(broker_addr, "pub-empty", 60).await;
    pub_empty
        .send(Packet::Publish {
            topic: String::new(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            payload: Bytes::from_static(b"bad"),
        })
        .await
        .unwrap();
    let closed_empty = timeout(Duration::from_secs(2), pub_empty.next())
        .await
        .expect("il broker non ha chiuso la connessione dopo PUBLISH su topic vuoto");
    assert!(closed_empty.is_none());

    // 3. Il subscriber su "a/b" non deve aver ricevuto nulla.
    let nothing = timeout(Duration::from_millis(200), sub_rx.recv()).await;
    assert!(
        nothing.is_err(),
        "il subscriber ha ricevuto un messaggio inatteso: {:?}",
        nothing
    );

    sub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_connect_protocol_level_5_receives_connack_0x01_and_closes() {
    use tokio::io::AsyncWriteExt;

    let broker_addr = start_test_broker().await;

    let mut stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    // Byte grezzi di un CONNECT MQTT 5.0 (livello 5): il nostro encoder rifiuta Mqtt50.
    let raw_v5_connect = [
        0x10, 0x0C, 0x00, 0x04, b'M', b'Q', b'T', b'T', 0x05, 0x02, 0x00, 0x3C, 0x00, 0x00,
    ];
    stream.write_all(&raw_v5_connect).await.unwrap();
    let mut framed = Framed::new(stream, MqttCodec::default());

    let response = timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("Timeout waiting for CONNACK 0x01")
        .expect("Connection closed before CONNACK")
        .expect("Protocol error");

    assert_eq!(
        response,
        Packet::ConnAck {
            session_present: false,
            return_code: ferrite_protocol::ConnectReturnCode::RefusedUnacceptableProtocolVersion,
        }
    );

    // Dopo CONNACK 0x01 il broker deve chiudere la socket.
    let closed = timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("Timeout waiting for socket close after CONNACK 0x01");
    assert!(closed.is_none());
}

#[tokio::test]
async fn test_two_anonymous_clients_with_clean_session_stay_connected() {
    let broker_addr = start_test_broker().await;

    // Primo client anonimo (client_id vuoto, clean_session = true) iscritto a "anon/topic"
    let mut anon1 = raw_connect(broker_addr, "", 60).await;
    anon1
        .send(Packet::Subscribe {
            packet_id: 1,
            topics: vec![ferrite_protocol::SubscribeTopic {
                filter: "anon/topic".into(),
                qos: QoS::AtMostOnce,
            }],
        })
        .await
        .unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(2), anon1.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Packet::SubAck { .. }
    ));

    // Secondo client anonimo (client_id vuoto, clean_session = true): non deve scalzare il primo
    let mut anon2 = raw_connect(broker_addr, "", 60).await;

    // Il secondo client pubblica su "anon/topic": il primo deve ancora riceverlo
    anon2
        .send(Packet::Publish {
            topic: "anon/topic".into(),
            qos: QoS::AtMostOnce,
            retain: false,
            dup: false,
            packet_id: None,
            payload: Bytes::from_static(b"hello-anon"),
        })
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), anon1.next())
        .await
        .expect("Timeout waiting for PUBLISH on first anonymous client")
        .expect("First anonymous client was unexpectedly disconnected")
        .expect("Protocol error");
    match received {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "anon/topic");
            assert_eq!(payload, Bytes::from_static(b"hello-anon"));
        }
        other => panic!("Expected Publish, got {:?}", other),
    }
}

#[tokio::test]
async fn test_empty_client_id_with_clean_session_false_returns_connack_0x02_and_closes() {
    let broker_addr = start_test_broker().await;

    let stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    let mut framed = Framed::new(stream, MqttCodec::default());

    framed
        .send(Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt311,
            clean_session: false,
            keep_alive: 60,
            client_id: String::new(),
            will: None,
            username: None,
            password: None,
        })
        .await
        .unwrap();

    let response = timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("Timeout waiting for CONNACK 0x02")
        .expect("Connection closed before CONNACK")
        .expect("Protocol error");

    assert_eq!(
        response,
        Packet::ConnAck {
            session_present: false,
            return_code: ferrite_protocol::ConnectReturnCode::RefusedIdentifierRejected,
        }
    );

    let closed = timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("Timeout waiting for socket close after CONNACK 0x02");
    assert!(closed.is_none());
}

#[tokio::test]
async fn test_second_connect_closes_connection() {
    let broker_addr = start_test_broker().await;

    let mut client = raw_connect(broker_addr, "second-connect-client", 60).await;

    // Secondo CONNECT sulla stessa connessione: violazione MQTT-3.1.0-2.
    client
        .send(Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt311,
            clean_session: true,
            keep_alive: 60,
            client_id: "second-connect-client".into(),
            will: None,
            username: None,
            password: None,
        })
        .await
        .unwrap();

    let closed = timeout(Duration::from_secs(2), client.next())
        .await
        .expect("il broker non ha chiuso la connessione dopo un secondo CONNECT");
    assert!(closed.is_none());
}

#[tokio::test]
async fn test_forwarded_publish_clears_retain_and_dup_from_qos0_publisher() {
    let broker_addr = start_test_broker().await;

    let (sub_client, mut sub_rx) = connect(MqttOptions::new("sub-retain-qos0", broker_addr))
        .await
        .unwrap();
    sub_client.subscribe("t/x", QoS::AtMostOnce).await.unwrap();

    let mut pub_raw = raw_connect(broker_addr, "pub-retain-qos0", 60).await;
    pub_raw
        .send(Packet::Publish {
            topic: "t/x".into(),
            qos: QoS::AtMostOnce,
            retain: true,
            dup: false,
            packet_id: None,
            payload: Bytes::from_static(b"hello"),
        })
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("Timeout waiting for forwarded PUBLISH")
        .expect("Channel closed");
    match received {
        Packet::Publish {
            topic,
            retain,
            dup,
            payload,
            ..
        } => {
            assert_eq!(topic, "t/x");
            assert_eq!(payload, Bytes::from_static(b"hello"));
            assert!(
                !retain,
                "RETAIN deve essere false nell'inoltro (MQTT-3.3.1-9)"
            );
            assert!(!dup, "DUP deve essere false nell'inoltro (MQTT-3.3.1-2)");
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    sub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_forwarded_publish_clears_retain_and_dup_from_qos1_publisher() {
    let broker_addr = start_test_broker().await;

    let (sub_client, mut sub_rx) = connect(MqttOptions::new("sub-retain-qos1", broker_addr))
        .await
        .unwrap();
    sub_client.subscribe("t/x", QoS::AtLeastOnce).await.unwrap();

    let mut pub_raw = raw_connect(broker_addr, "pub-retain-qos1", 60).await;
    pub_raw
        .send(Packet::Publish {
            topic: "t/x".into(),
            qos: QoS::AtLeastOnce,
            retain: true,
            dup: true,
            packet_id: Some(1),
            payload: Bytes::from_static(b"hello-qos1"),
        })
        .await
        .unwrap();

    let puback = timeout(Duration::from_secs(2), pub_raw.next())
        .await
        .expect("Timeout waiting for PUBACK")
        .expect("Connection closed")
        .expect("Protocol error");
    assert_eq!(puback, Packet::PubAck { packet_id: 1 });

    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("Timeout waiting for forwarded PUBLISH")
        .expect("Channel closed");
    match received {
        Packet::Publish {
            topic,
            qos,
            retain,
            dup,
            payload,
            ..
        } => {
            assert_eq!(topic, "t/x");
            assert_eq!(payload, Bytes::from_static(b"hello-qos1"));
            assert_eq!(qos, QoS::AtMostOnce);
            assert!(
                !retain,
                "RETAIN deve essere false nell'inoltro (MQTT-3.3.1-9)"
            );
            assert!(!dup, "DUP deve essere false nell'inoltro (MQTT-3.3.1-2)");
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    sub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_publish_qos2_closes_connection_and_does_not_deliver() {
    let broker_addr = start_test_broker().await;

    // Subscriber S iscritto a "t/q2"
    let (sub_client, mut sub_rx) = connect(MqttOptions::new("sub-q2", broker_addr))
        .await
        .unwrap();
    sub_client.subscribe("t/q2", QoS::AtMostOnce).await.unwrap();

    // Publisher P invia PUBLISH QoS 2 con packet_id = 1 su "t/q2"
    let mut pub_q2 = raw_connect(broker_addr, "pub-q2", 60).await;
    pub_q2
        .send(Packet::Publish {
            topic: "t/q2".into(),
            qos: QoS::ExactlyOnce,
            retain: false,
            dup: false,
            packet_id: Some(1),
            payload: Bytes::from_static(b"qos2-not-supported"),
        })
        .await
        .unwrap();

    // La connessione di P deve essere chiusa entro 2 secondi (EOF o errore)
    let closed = timeout(Duration::from_secs(2), pub_q2.next())
        .await
        .expect("il broker non ha chiuso la connessione dopo PUBLISH QoS 2");
    assert!(
        matches!(closed, None | Some(Err(_))),
        "attesa chiusura connessione dopo PUBLISH QoS 2, ricevuto: {:?}",
        closed
    );

    // S non deve ricevere nulla entro 500 ms
    let nothing = timeout(Duration::from_millis(500), sub_rx.recv()).await;
    assert!(
        nothing.is_err(),
        "il subscriber ha ricevuto un messaggio QoS 2 inatteso: {:?}",
        nothing
    );

    // Un secondo publisher QoS 1 su "t/q2" deve continuare a funzionare
    let (pub_q1, _pub_q1_rx) = connect(MqttOptions::new("pub-q1", broker_addr))
        .await
        .unwrap();
    timeout(
        Duration::from_secs(2),
        pub_q1.publish_qos1("t/q2", Bytes::from_static(b"qos1-ok")),
    )
    .await
    .expect("Timeout waiting for QoS 1 PUBACK")
    .expect("publish_qos1 failed");

    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("Timeout waiting for QoS 1 PUBLISH")
        .expect("Channel closed");
    match received {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "t/q2");
            assert_eq!(payload, Bytes::from_static(b"qos1-ok"));
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    pub_q1.disconnect().await.unwrap();
    sub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_takeover_stress_leaves_no_orphan_subscriptions() {
    use bytes::BytesMut;
    use tokio::io::AsyncWriteExt;
    use tokio_util::codec::Encoder;

    let broker_addr = start_test_broker().await;

    // Pre-codifichiamo CONNECT + SUBSCRIBE "race/#" in un unico buffer per inviarli in pipeline.
    let mut codec = MqttCodec::default();
    let mut pipelined = BytesMut::new();
    codec
        .encode(
            Packet::Connect {
                protocol_version: ProtocolVersion::Mqtt311,
                clean_session: true,
                keep_alive: 60,
                client_id: "dup".into(),
                will: None,
                username: None,
                password: None,
            },
            &mut pipelined,
        )
        .unwrap();
    codec
        .encode(
            Packet::Subscribe {
                packet_id: 1,
                topics: vec![ferrite_protocol::SubscribeTopic {
                    filter: "race/#".into(),
                    qos: QoS::AtMostOnce,
                }],
            },
            &mut pipelined,
        )
        .unwrap();
    let pipelined_bytes = pipelined.freeze();

    for _ in 0..50 {
        // Socket A invia CONNECT + SUBSCRIBE "race/#" in un solo write_all
        let mut sock_a = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
        sock_a.write_all(&pipelined_bytes).await.unwrap();

        // Socket B fa subito takeover dello stesso client_id "dup"
        let mut sock_b = raw_connect(broker_addr, "dup", 60).await;

        // Attendiamo che A venga chiusa dal broker (o al più 2s)
        let mut framed_a = Framed::new(sock_a, MqttCodec::default());
        let _ = timeout(Duration::from_secs(2), async {
            while framed_a.next().await.is_some() {}
        })
        .await;

        // Chiudiamo B con DISCONNECT pulito
        let _ = sock_b.send(Packet::Disconnect).await;
    }

    // Connessione finale pulita con lo stesso client_id "dup", SENZA alcuna SUBSCRIBE.
    // Se una vecchia connessione avesse lasciato "race/#" orfano nel trie, riceverebbe il messaggio.
    let (final_dup, mut dup_rx) = connect(MqttOptions::new("dup", broker_addr)).await.unwrap();
    let (pub_client, _pub_rx) = connect(MqttOptions::new("race-publisher", broker_addr))
        .await
        .unwrap();
    pub_client
        .publish("race/x", Bytes::from_static(b"should-not-arrive"))
        .await
        .unwrap();

    let nothing = timeout(Duration::from_millis(300), dup_rx.recv()).await;
    assert!(
        nothing.is_err(),
        "il client 'dup' ha ricevuto un messaggio da una sottoscrizione orfana: {:?}",
        nothing
    );

    pub_client.disconnect().await.unwrap();
    final_dup.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_broker_unsubscribe_removes_only_requested_filter() {
    let broker_addr = start_test_broker().await;

    let mut sub = raw_connect(broker_addr, "sub-unsub", 60).await;
    sub.send(Packet::Subscribe {
        packet_id: 1,
        topics: vec![
            ferrite_protocol::SubscribeTopic {
                filter: "a/b".into(),
                qos: QoS::AtMostOnce,
            },
            ferrite_protocol::SubscribeTopic {
                filter: "c/d".into(),
                qos: QoS::AtMostOnce,
            },
        ],
    })
    .await
    .unwrap();
    assert!(matches!(
        timeout(Duration::from_secs(2), sub.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        Packet::SubAck { packet_id: 1, .. }
    ));

    // UNSUBSCRIBE solo da "a/b"
    sub.send(Packet::Unsubscribe {
        packet_id: 42,
        filters: vec!["a/b".into()],
    })
    .await
    .unwrap();
    let unsuback = timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("Timeout waiting for UNSUBACK")
        .expect("Connection closed")
        .expect("Protocol error");
    assert_eq!(unsuback, Packet::UnsubAck { packet_id: 42 });

    let (pub_client, _pub_rx) = connect(MqttOptions::new("pub-unsub", broker_addr))
        .await
        .unwrap();
    pub_client
        .publish("a/b", Bytes::from_static(b"dropped"))
        .await
        .unwrap();
    pub_client
        .publish("c/d", Bytes::from_static(b"delivered"))
        .await
        .unwrap();

    // Deve arrivare solo "c/d", non "a/b".
    let received = timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("Timeout waiting for PUBLISH on c/d")
        .expect("Connection closed")
        .expect("Protocol error");
    match received {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "c/d");
            assert_eq!(payload, Bytes::from_static(b"delivered"));
        }
        other => panic!("Expected Publish on c/d, got {:?}", other),
    }

    pub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_broker_unsubscribe_unknown_filter_returns_unsuback_and_stays_open() {
    let broker_addr = start_test_broker().await;

    let mut sub = raw_connect(broker_addr, "sub-unsub-unknown", 60).await;

    // UNSUBSCRIBE di un filtro mai sottoscritto: MQTT-3.10.4-5 richiede UNSUBACK ugualmente.
    sub.send(Packet::Unsubscribe {
        packet_id: 99,
        filters: vec!["never/subscribed".into()],
    })
    .await
    .unwrap();
    let unsuback = timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("Timeout waiting for UNSUBACK")
        .expect("Connection closed")
        .expect("Protocol error");
    assert_eq!(unsuback, Packet::UnsubAck { packet_id: 99 });

    // La connessione deve restare aperta (verifichiamo con PINGREQ -> PINGRESP).
    sub.send(Packet::PingReq).await.unwrap();
    let pingresp = timeout(Duration::from_secs(2), sub.next())
        .await
        .expect("Timeout waiting for PINGRESP")
        .expect("Connection closed after UNSUBSCRIBE of unknown filter")
        .expect("Protocol error");
    assert_eq!(pingresp, Packet::PingResp);
}

#[tokio::test]
async fn test_client_unsubscribe_waits_for_unsuback_and_stops_receiving() {
    let broker_addr = start_test_broker().await;

    let (sub_client, mut sub_rx) = connect(MqttOptions::new("client-unsub", broker_addr))
        .await
        .unwrap();
    sub_client
        .subscribe("updates/news", QoS::AtMostOnce)
        .await
        .unwrap();

    let (pub_client, _pub_rx) = connect(MqttOptions::new("pub-news", broker_addr))
        .await
        .unwrap();

    // 1. Prima dell'unsubscribe il messaggio arriva normalmente
    pub_client
        .publish("updates/news", Bytes::from_static(b"msg-1"))
        .await
        .unwrap();
    let first = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("Timeout waiting for first PUBLISH")
        .expect("Channel closed");
    match first {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "updates/news");
            assert_eq!(payload, Bytes::from_static(b"msg-1"));
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    // 2. Unsubscribe attende UNSUBACK (che non viene inoltrato a sub_rx)
    timeout(
        Duration::from_secs(2),
        sub_client.unsubscribe("updates/news"),
    )
    .await
    .expect("Timeout waiting for unsubscribe UNSUBACK")
    .expect("unsubscribe failed");

    // 3. Dopo l'unsubscribe nessun messaggio (e nessun UNSUBACK) deve arrivare su sub_rx
    pub_client
        .publish("updates/news", Bytes::from_static(b"msg-2"))
        .await
        .unwrap();
    let nothing = timeout(Duration::from_millis(300), sub_rx.recv()).await;
    assert!(
        nothing.is_err(),
        "ricevuto pacchetto inatteso dopo unsubscribe: {:?}",
        nothing
    );

    pub_client.disconnect().await.unwrap();
    sub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_survives_dropped_incoming_receiver() {
    let broker_addr = start_test_broker().await;

    let mut opts = MqttOptions::new("drop-rx-client", broker_addr);
    opts.keep_alive = 1;
    let (client, incoming_rx) = connect(opts).await.unwrap();

    // Scarta subito il ricevitore: il driver deve continuare a gestire PINGREQ/PINGRESP.
    drop(incoming_rx);

    // Oltre 1.5 * keep_alive (1.5s): il broker disconnetterebbe se i ping smettessero.
    tokio::time::sleep(Duration::from_millis(2500)).await;

    let res = timeout(
        Duration::from_secs(2),
        client.publish_qos1("keep/alive", Bytes::from_static(b"ok")),
    )
    .await
    .expect("publish_qos1 timed out after dropping incoming_rx");
    res.expect("publish_qos1 failed after dropping incoming_rx");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_driver_not_blocked_by_full_incoming_channel() {
    let broker_addr = start_test_broker().await;

    let (sub_client, _held_rx) = connect(MqttOptions::new("slow-rx-client", broker_addr))
        .await
        .unwrap();
    sub_client
        .subscribe("flood/in", QoS::AtMostOnce)
        .await
        .unwrap();

    let (pub_client, _pub_rx) = connect(MqttOptions::new("flood-pub", broker_addr))
        .await
        .unwrap();
    for i in 0..300u32 {
        pub_client
            .publish("flood/in", Bytes::from(format!("msg-{i}")))
            .await
            .unwrap();
    }

    // Diamo tempo ai 300 messaggi di arrivare al driver del subscriber.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // Nonostante la coda incoming piena, il driver del subscriber deve gestire publish_qos1.
    let res = timeout(
        Duration::from_secs(2),
        sub_client.publish_qos1("other/topic", Bytes::from_static(b"still-alive")),
    )
    .await
    .expect("publish_qos1 timed out: driver bloccato dal canale incoming pieno");
    res.expect("publish_qos1 failed on slow subscriber");

    pub_client.disconnect().await.unwrap();
    sub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_does_not_forward_pingresp() {
    let broker_addr = start_test_broker().await;

    let mut opts = MqttOptions::new("no-pingresp-client", broker_addr);
    opts.keep_alive = 1;
    let (client, mut incoming_rx) = connect(opts).await.unwrap();

    // Leggiamo incoming_rx per 2.5s: il driver deve scambiare PINGREQ/PINGRESP internamente
    // senza mai inoltrare Packet::PingResp all'utente.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(2500);
    while let Ok(Some(packet)) = tokio::time::timeout_at(deadline, incoming_rx.recv()).await {
        assert_ne!(
            packet,
            Packet::PingResp,
            "PINGRESP non deve essere inoltrato sul canale incoming"
        );
    }

    // La connessione deve essere ancora viva dopo 2.5s (> 1.5 * keep_alive).
    timeout(
        Duration::from_secs(2),
        client.publish_qos1("ping/check", Bytes::from_static(b"alive")),
    )
    .await
    .expect("publish_qos1 timed out after keep-alive window")
    .expect("publish_qos1 failed");

    client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_oversized_connect_header_closes_connection_immediately() {
    use tokio::io::AsyncWriteExt;

    let broker_addr = start_test_broker().await;

    let mut stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    // Fixed header di un CONNECT con remaining_length = 100_000 (varint: 0xA0, 0x8D, 0x06),
    // oltre il limite pre-CONNECT (64 KiB), senza inviare il corpo.
    stream.write_all(&[0x10, 0xA0, 0x8D, 0x06]).await.unwrap();

    let mut framed = Framed::new(stream, MqttCodec::default());
    let closed = timeout(Duration::from_secs(2), framed.next())
        .await
        .expect("il broker non ha chiuso subito la connessione con header CONNECT > 64 KiB");
    assert!(closed.is_none());
}

#[tokio::test]
async fn test_large_publish_200kib_accepted_after_connect() {
    let broker_addr = start_test_broker().await;

    let (sub_client, mut sub_rx) = connect(MqttOptions::new("sub-200k", broker_addr))
        .await
        .unwrap();
    sub_client
        .subscribe("large/payload", QoS::AtMostOnce)
        .await
        .unwrap();

    let (pub_client, _pub_rx) = connect(MqttOptions::new("pub-200k", broker_addr))
        .await
        .unwrap();
    let payload = Bytes::from(vec![0x5Au8; 200 * 1024]);
    pub_client
        .publish("large/payload", payload.clone())
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("Timeout waiting for 200 KiB PUBLISH")
        .expect("Channel closed");
    match received {
        Packet::Publish {
            topic,
            payload: got,
            ..
        } => {
            assert_eq!(topic, "large/payload");
            assert_eq!(got.len(), 200 * 1024);
            assert_eq!(got, payload);
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    pub_client.disconnect().await.unwrap();
    sub_client.disconnect().await.unwrap();
}

#[tokio::test]
async fn test_client_subscribe_refused_and_valid_immediate_delivery() {
    let broker_addr = start_test_broker().await;

    let (sub_client, mut sub_rx) = connect(MqttOptions::new("sub-refuse-real", broker_addr))
        .await
        .unwrap();

    // 1. Filtro invalido "a/#/b" -> SubscriptionRefused
    let refused = timeout(
        Duration::from_secs(2),
        sub_client.subscribe("a/#/b", QoS::AtMostOnce),
    )
    .await
    .expect("timeout su subscribe invalida");
    assert!(
        matches!(
            refused,
            Err(ferrite_client::ClientError::SubscriptionRefused)
        ),
        "atteso SubscriptionRefused per 'a/#/b', ottenuto {:?}",
        refused
    );

    // 2. Subito dopo sulla stessa connessione, subscribe su "a/b" riesce e restituisce il QoS concesso.
    let granted = timeout(
        Duration::from_secs(2),
        sub_client.subscribe("a/b", QoS::AtLeastOnce),
    )
    .await
    .expect("timeout su subscribe valida")
    .expect("subscribe valida fallita");
    assert_eq!(granted, QoS::AtMostOnce);

    // 3. Publish immediata da un altro client (senza sleep!) deve essere ricevuta.
    let (pub_client, _pub_rx) = connect(MqttOptions::new("pub-immediate", broker_addr))
        .await
        .unwrap();
    pub_client
        .publish("a/b", Bytes::from_static(b"instant"))
        .await
        .unwrap();

    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("timeout in attesa del PUBLISH immediato")
        .expect("canale chiuso");
    match received {
        Packet::Publish { topic, payload, .. } => {
            assert_eq!(topic, "a/b");
            assert_eq!(payload, Bytes::from_static(b"instant"));
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    pub_client.disconnect().await.unwrap();
    sub_client.disconnect().await.unwrap();
}
