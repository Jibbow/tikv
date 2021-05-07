// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

#[test]
fn test_coprocessor_not_found() {
    let (_cluster, client, ctx) = must_new_cluster_and_kv_client();

    let mut req = RawCoprocessorRequest::new();
    req.set_context(ctx.clone());
    req.set_copr_name("nonexistent-plugin-name");

    let resp = client.coprocessor_v2(req);

    assert_true!(resp.has_other_error());
    assert_false!(resp.has_data());
}

#[test]
fn test_coprocessor_version_mismatch() {
    assert!(false);
}

#[test]
fn test_invalid_raw_request() {
    assert!(false);
}

#[test]
fn test_simple_request() {
    assert!(false);
}

#[test]
fn test_storage_interaction() {
    assert!(false);
}

#[test]
fn test_storage_error() {
    assert!(false);
}

#[test]
fn test_coprocessor_error() {
    assert!(false);
}

#[test]
fn test_coprocessor_panics() {
    assert!(false);
}
