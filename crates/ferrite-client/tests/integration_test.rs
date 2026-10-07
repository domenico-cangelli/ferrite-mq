use bytes::Bytes;
use ferrite_broker_core::run_server;
use ferrite_client::{connect, MqttOptions, Packet, QoS};
use std::net::SocketAddr;
use std::time::Duration;
use futures::{SinkExt, StreamExt};
use tokio::net::TcpListener;
use tokio::time::timeout;
use tokio_util::codec::Framed;
use ferrite_protocol::{MqttCodec, ProtocolVersion};

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
        Err(_) => panic!("Il broker non ha disconnesso il client entro la finestra temporale prevista"),
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

    assert!(pub_result.is_ok(), "publish_qos1 timed out waiting for PUBACK");
    pub_result.unwrap().expect("publish_qos1 failed");

    // 4. Verifica che il subscriber riceva il payload
    let received = timeout(Duration::from_secs(2), sub_rx.recv())
        .await
        .unwrap()
        .unwrap();

    match received {
        Packet::Publish { topic, payload: p, .. } => {
            assert_eq!(topic, "orders/processed");
            assert_eq!(p, payload);
        }
        other => panic!("Expected Publish, got {:?}", other),
    }

    sub_client.disconnect().await.unwrap();
    pub_client.disconnect().await.unwrap();
}