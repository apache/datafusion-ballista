// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Secrets in the session config must not reach the logs at any level.
//!
//! These tests live in their own binary because they install a process-wide
//! logger. Each test uses secret values no other test uses, so the tests can
//! share the captured lines and still run in parallel.

#![cfg(feature = "build-binary")]

use std::sync::{Mutex, Once};

use ballista_core::extension::{SessionConfigExt, SessionConfigHelperExt};
use ballista_core::object_store::{
    CustomObjectStoreRegistry, S3Options, session_config_with_s3_support,
};
use ballista_core::serde::protobuf::KeyValuePair;
use datafusion::config::ExtensionOptions;
use datafusion::execution::object_store::ObjectStoreRegistry;
use datafusion::prelude::SessionConfig;
use url::Url;

static LINES: Mutex<Vec<String>> = Mutex::new(Vec::new());

struct CaptureLogger;

impl log::Log for CaptureLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        LINES.lock().unwrap().push(record.args().to_string());
    }

    fn flush(&self) {}
}

fn capture_logs() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        log::set_logger(&CaptureLogger).unwrap();
        log::set_max_level(log::LevelFilter::Trace);
    });
}

fn lines_containing(needle: &str) -> Vec<String> {
    LINES
        .lock()
        .unwrap()
        .iter()
        .filter(|line| line.contains(needle))
        .cloned()
        .collect()
}

fn assert_not_logged(secret: &str) {
    let leaked = lines_containing(secret);
    assert!(leaked.is_empty(), "secret was logged: {leaked:?}");
}

fn kv(key: &str, value: &str) -> KeyValuePair {
    KeyValuePair {
        key: key.to_string(),
        value: Some(value.to_string()),
    }
}

#[test]
fn applying_session_config_does_not_log_secrets() {
    capture_logs();

    let _ = session_config_with_s3_support().update_from_key_value_pair(&[
        kv("s3.secret_access_key", "APPLY_SECRET"),
        kv("s3.session_token", "APPLY_TOKEN"),
    ]);

    assert_not_logged("APPLY_SECRET");
    assert_not_logged("APPLY_TOKEN");
}

#[test]
fn sending_session_config_does_not_log_secrets() {
    capture_logs();

    let config = session_config_with_s3_support().update_from_key_value_pair(&[
        kv("s3.secret_access_key", "SEND_SECRET"),
        kv("s3.session_token", "SEND_TOKEN"),
    ]);
    LINES.lock().unwrap().retain(|line| !line.contains("SEND_"));

    let pairs = config.to_key_value_pairs();

    // the values still travel, only the log lines are redacted
    assert!(pairs.contains(&kv("s3.secret_access_key", "SEND_SECRET")));
    assert!(pairs.contains(&kv("s3.session_token", "SEND_TOKEN")));
    assert_not_logged("SEND_SECRET");
    assert_not_logged("SEND_TOKEN");
}

#[test]
fn rejected_setting_does_not_log_secrets() {
    capture_logs();

    // no `S3Options` registered, so the `s3.*` keys cannot be applied
    let _ = SessionConfig::new_with_ballista().update_from_key_value_pair(&[
        kv("s3.secret_access_key", "REJECTED_SECRET"),
        kv("s3.session_token", "REJECTED_TOKEN"),
    ]);

    assert_not_logged("REJECTED_SECRET");
    assert_not_logged("REJECTED_TOKEN");
}

#[test]
fn unknown_s3_key_does_not_log_its_value() {
    capture_logs();

    let mut options = S3Options::default();
    // a misspelling that no longer looks like the name of a secret
    assert!(options.set("session_tokn", "MISSPELLED_SECRET").is_err());

    assert_not_logged("MISSPELLED_SECRET");
}

#[test]
fn get_store_does_not_log_secrets() {
    capture_logs();

    let mut options = S3Options::default();
    options.set("access_key_id", "STORE_KEY_ID").unwrap();
    options.set("secret_access_key", "STORE_SECRET").unwrap();
    options.set("session_token", "STORE_TOKEN").unwrap();
    options.set("region", "us-east-1").unwrap();
    let registry = CustomObjectStoreRegistry::new(options);

    registry
        .get_store(&Url::parse("s3://bucket").unwrap())
        .unwrap();

    assert_not_logged("STORE_SECRET");
    assert_not_logged("STORE_TOKEN");
}

#[test]
fn s3_options_debug_redacts_secrets() {
    let mut options = S3Options::default();
    options.set("access_key_id", "DEBUG_KEY_ID").unwrap();
    options.set("secret_access_key", "DEBUG_SECRET").unwrap();
    options.set("session_token", "DEBUG_TOKEN").unwrap();
    options.set("region", "eu-west-1").unwrap();

    let debug = format!("{options:?}");

    assert!(!debug.contains("DEBUG_SECRET"), "{debug}");
    assert!(!debug.contains("DEBUG_TOKEN"), "{debug}");
    assert!(debug.contains("<redacted (length: 12)>"), "{debug}");
    assert!(debug.contains("<redacted (length: 11)>"), "{debug}");
    // what is not secret stays readable
    assert!(debug.contains("DEBUG_KEY_ID"), "{debug}");
    assert!(debug.contains("eu-west-1"), "{debug}");
}

#[test]
fn non_secret_settings_are_still_logged() {
    capture_logs();

    let config = session_config_with_s3_support()
        .update_from_key_value_pair(&[kv("s3.region", "ap-visible-1")]);
    let _ = config.to_key_value_pairs();

    let logged = lines_containing("ap-visible-1");
    assert!(
        logged.iter().any(|line| line.contains("setting up")),
        "{logged:?}"
    );
    assert!(
        logged.iter().any(|line| line.contains("sending")),
        "{logged:?}"
    );
}
