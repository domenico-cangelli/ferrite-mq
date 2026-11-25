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

    // Iscriviti al topic
    sub_client
        .subscribe("factory/sensors/temperature", QoS::AtMostOnce)
        .await
        .expect("Failed to subscribe");

    // 3. Attendi la ricezione del SUBACK
    let suback = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .expect("Timeout waiting for SUBACK")
        .expect("Channel closed");

    assert!(matches!(suback, Packet::SubAck { .. }));

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

    // Impostiamo un keep_alive brevissimo (1 secondo)
    let mut opts = MqttOptions::new("ping-client", broker_addr);
    opts.keep_alive = 1;

    let (_client, mut incoming_rx) = connect(opts).await.expect("Connection failed");

    // In 2 secondi, il client deve aver emesso almeno un PINGREQ e aver ricevuto PINGRESP dal broker
    let packet = timeout(Duration::from_secs(2), incoming_rx.recv())
        .await
        .expect("Timeout waiting for PINGRESP")
        .expect("Channel closed");

    assert_eq!(packet, Packet::PingResp);
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

    // Attendi SUBACK
    let suback = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(suback, Packet::SubAck { .. }));

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
    let suback = timeout(Duration::from_secs(2), new_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(suback, Packet::SubAck { .. }));

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
        let suback = rx_c.recv().await.expect("client C channel closed");
        assert!(matches!(suback, Packet::SubAck { .. }));

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
    let suback = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(suback, Packet::SubAck { .. }));

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
    let broker_addr = start_test_broker().await;

    let stream = tokio::net::TcpStream::connect(broker_addr).await.unwrap();
    let mut framed = Framed::new(stream, MqttCodec::default());

    framed
        .send(Packet::Connect {
            protocol_version: ProtocolVersion::Mqtt50,
            clean_session: true,
            keep_alive: 60,
            client_id: "mqtt5-client".into(),
            will: None,
            username: None,
            password: None,
        })
        .await
        .unwrap();

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
    let suback = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(suback, Packet::SubAck { .. }));

    let mut pub_raw = raw_connect(broker_addr, "pub-retain-qos0", 60).await;
    pub_raw
        .send(Packet::Publish {
            topic: "t/x".into(),
            qos: QoS::AtMostOnce,
            retain: true,
            dup: true,
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
    let suback = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(suback, Packet::SubAck { .. }));

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
