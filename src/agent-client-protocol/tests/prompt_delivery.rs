//! Typed SDK dispatch for the experimental v2 prompt-delivery field.

#![cfg(feature = "unstable_prompt_delivery")]

use agent_client_protocol::JsonRpcMessage;
use agent_client_protocol::v2::schema::{ClientRequest, PromptDelivery};
use serde_json::json;

#[test]
fn prompt_delivery_uses_the_existing_prompt_dispatch() {
    for (name, mode) in [
        ("steer", PromptDelivery::Steer),
        ("queue", PromptDelivery::Queue),
    ] {
        let params = json!({
            "sessionId": "session",
            "prompt": [{"type": "text", "text": "change course"}],
            "delivery": name,
            "idempotencyKey": "one-submission"
        });
        let parsed = ClientRequest::parse_message("session/prompt", &params).unwrap();
        assert_eq!(parsed.to_untyped_message().unwrap().params(), &params);
        let ClientRequest::PromptRequest(request) = parsed else {
            panic!("expected a normal prompt request");
        };
        assert_eq!(request.delivery, Some(mode));
    }
}

#[test]
fn unknown_delivery_mode_is_invalid_params() {
    let params = json!({
        "sessionId": "session",
        "prompt": [{"type": "text", "text": "change course"}],
        "delivery": "interrupt"
    });
    let error = ClientRequest::parse_message("session/prompt", &params).unwrap_err();
    assert_eq!(i32::from(error.code), -32602);
}
