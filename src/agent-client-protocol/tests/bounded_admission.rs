use agent_client_protocol::{
    BoundedChannel, ChannelLimits, ConnectionTo, Dispatch, Handled, TransportFrame, UntypedMessage,
    UntypedRole, on_receive_dispatch,
};
use futures::StreamExt as _;
use serde_json::{Value, json};
use std::time::Duration;

fn limits() -> ChannelLimits {
    ChannelLimits {
        max_frame_bytes: 512,
        max_buffered_bytes: 4096,
        max_buffered_frames: 8,
        max_pending_requests: 1,
        max_tasks: 4,
    }
}

async fn receive(peer: &mut BoundedChannel) -> Value {
    let frame = peer.rx.next().await.expect("one frame");
    serde_json::from_slice(frame.as_bytes()).expect("one JSON value")
}

#[tokio::test]
async fn bounded_transport_preserves_string_ids_and_rejects_arrays_as_one_message() {
    let (transport, mut peer) = BoundedChannel::duplex(limits()).unwrap();
    let app = UntypedRole.builder().on_receive_dispatch(
        async |dispatch: Dispatch, _: ConnectionTo<UntypedRole>| match dispatch {
            Dispatch::Request(request, responder) if request.method() == "echo" => {
                responder.respond(request.params().clone())?;
                Ok(Handled::Yes)
            }
            message => Ok(Handled::No {
                message,
                retry: false,
            }),
        },
        on_receive_dispatch!(),
    );
    let wire = async move {
        peer.tx
            .try_send_serialized(
                r#"{"jsonrpc":"2.0","id":"wire:id","method":"echo","params":{"a":1}}"#,
            )
            .unwrap();
        assert_eq!(
            receive(&mut peer).await,
            json!({"jsonrpc":"2.0","id":"wire:id","result":{"a":1}})
        );
        peer.tx
            .try_send_serialized(r#"[{"jsonrpc":"2.0","id":"must-not-dispatch","method":"echo"}]"#)
            .unwrap();
        let rejection = receive(&mut peer).await;
        assert_eq!(rejection["id"], Value::Null);
        assert_eq!(rejection["error"]["code"], -32600);
        peer.tx.close_channel();
        assert!(peer.rx.next().await.is_none());
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(app.connect_to(transport), wire)
    })
    .await
    .unwrap();
    result.unwrap();
}

#[tokio::test]
async fn bounded_escaped_responder_keeps_request_admitted_until_released() {
    let limits = ChannelLimits {
        max_buffered_frames: 1,
        ..limits()
    };
    let (transport, peer) = BoundedChannel::duplex(limits).unwrap();
    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let app = UntypedRole.builder().on_receive_dispatch(
        async move |dispatch: Dispatch, _: ConnectionTo<UntypedRole>| {
            tx.send(dispatch).unwrap();
            Ok(Handled::Yes)
        },
        on_receive_dispatch!(),
    );
    let wire = async move {
        let mut rx = rx;
        peer.tx
            .try_send_serialized(r#"{"jsonrpc":"2.0","id":4,"method":"held"}"#)
            .unwrap();
        let held = rx.recv().await.expect("retained dispatch");
        assert!(matches!(held, Dispatch::Request(..)));
        let error = peer
            .tx
            .try_send_serialized(r#"{"jsonrpc":"2.0","method":"overflow"}"#)
            .unwrap_err();
        assert!(
            error.to_string().contains("admission exhausted"),
            "{error:?}"
        );
        drop(held);
    };
    let (result, ()) = tokio::time::timeout(Duration::from_secs(3), async {
        tokio::join!(app.connect_to(transport), wire)
    })
    .await
    .unwrap();
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains("admission exhausted")
    );
}

#[tokio::test]
async fn bounded_pending_request_limit_is_terminal_and_prevents_further_publication() {
    let (transport, _peer) = BoundedChannel::duplex(limits()).unwrap();
    let error = UntypedRole
        .builder()
        .connect_with(transport, async |cx| {
            let first = cx.send_request(UntypedMessage::new("first", json!({})).unwrap());
            let second = cx.send_request(UntypedMessage::new("second", json!({})).unwrap());
            // Failure is synchronous, even when the actor has not polled the queue.
            let later = cx.send_notification(UntypedMessage::new("later", json!({})).unwrap());
            let error = later.unwrap_err();
            assert!(
                error.to_string().contains("pending request admission"),
                "{error:?}"
            );
            drop((first, second));
            Ok(())
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("pending request admission"));
}

#[tokio::test]
async fn bounded_raw_frames_route_original_request_cancellation() {
    let (transport, mut peer) = BoundedChannel::duplex(limits()).unwrap();
    let app = UntypedRole.builder().connect_with(transport, async |cx| {
        let request = cx.send_request(UntypedMessage::new("request", json!({})).unwrap());
        request.cancel()?;
        let error = request.block_task().await.unwrap_err();
        assert_eq!(
            error.code,
            agent_client_protocol::Error::request_cancelled().code
        );
        Ok(())
    });
    let wire = async move {
        let request = receive(&mut peer).await;
        assert_eq!(request["method"], "request");
        let cancellation = receive(&mut peer).await;
        assert_eq!(cancellation["method"], "$/cancel_request");
        assert_eq!(cancellation["params"]["requestId"], request["id"]);
        let response = json!({
            "jsonrpc":"2.0", "id": request["id"],
            "error": {"code": -32800, "message": "Request cancelled"}
        });
        peer.tx
            .try_send(TransportFrame::parse_json(&response.to_string()))
            .unwrap();
        assert!(peer.rx.next().await.is_none());
    };
    let (result, ()) =
        tokio::time::timeout(Duration::from_secs(3), async { tokio::join!(app, wire) })
            .await
            .unwrap();
    result.unwrap();
}

#[tokio::test]
async fn bounded_failure_requests_and_awaits_cooperative_finish() {
    use agent_client_protocol::{Channel, ConnectTo, ConnectionDriver, Error, TransportChannel};
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    struct Transport {
        endpoint: BoundedChannel,
        driver: ConnectionDriver,
    }
    impl ConnectTo<UntypedRole> for Transport {
        async fn connect_to(self, _: impl ConnectTo<UntypedRole>) -> Result<(), Error> {
            unreachable!("test uses normalization only")
        }

        fn into_channel_and_future(self) -> (Channel, Option<ConnectionDriver>) {
            unreachable!("a bounded endpoint must not be normalized as a Channel")
        }

        fn into_transport_and_future(self) -> (TransportChannel, Option<ConnectionDriver>) {
            (TransportChannel::Bounded(self.endpoint), Some(self.driver))
        }
    }

    for prefailed in [false, true] {
        let (endpoint, peer) = BoundedChannel::duplex(limits()).unwrap();
        if prefailed {
            drop(endpoint.tx.fail("prefailed bounded limit"));
        }
        let cleaned = Arc::new(AtomicBool::new(false));
        let (finish_tx, finish_rx) = tokio::sync::oneshot::channel();
        let driver = ConnectionDriver::with_finish(
            {
                let cleaned = cleaned.clone();
                async move {
                    finish_rx.await.unwrap();
                    tokio::task::yield_now().await;
                    cleaned.store(true, Ordering::Release);
                    drop(peer);
                    Ok(())
                }
            },
            move || {
                finish_tx.send(()).unwrap();
            },
        );

        let result = tokio::time::timeout(
            Duration::from_secs(3),
            UntypedRole
                .builder()
                .connect_with(Transport { endpoint, driver }, async |cx| {
                    let oversized =
                        UntypedMessage::new("oversized", json!({"value": "x".repeat(1024)}))
                            .unwrap();
                    cx.send_notification(oversized).unwrap_err();
                    std::future::pending::<Result<(), Error>>().await
                }),
        )
        .await
        .unwrap();
        assert!(result.unwrap_err().to_string().contains("bounded limit"));
        assert!(cleaned.load(Ordering::Acquire));
    }
}
