use bytes::Bytes;
use ferrite_broker_core::run_server;
use ferrite_client::{connect, MqttOptions, Packet, QoS};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::time::timeout;

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