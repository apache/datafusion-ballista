# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import pytest

from ballista._internal_ballista import (
    BallistaConfigExtension,
    ballista_datafusion_config_defaults,
)
from ballista.extension import (
    _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS,
    BallistaSessionConfig,
)
from datafusion import SessionConfig, SessionContext


def _settings(config: SessionConfig) -> dict:
    ctx = SessionContext(config)
    ctx.sql("SET datafusion.catalog.information_schema = true").collect()
    rows = ctx.sql("SELECT name, value FROM information_schema.df_settings").to_pylist()
    return {row["name"]: row["value"] for row in rows}


def test_restricted_options_set_without_config():
    defaults = ballista_datafusion_config_defaults()
    settings = _settings(BallistaSessionConfig().build())

    for key in _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS:
        assert settings[key] == defaults[key], key


def test_restricted_options_override_session_config():
    config = SessionConfig().set("datafusion.optimizer.prefer_hash_join", "true")
    settings = _settings(BallistaSessionConfig(config).build())

    assert settings["datafusion.optimizer.prefer_hash_join"] == "false"


def test_restricted_options_override_set():
    settings = _settings(
        BallistaSessionConfig()
        .set("datafusion.optimizer.prefer_hash_join", True)
        .set("datafusion.optimizer.enable_round_robin_repartition", True)
        .build()
    )

    assert settings["datafusion.optimizer.prefer_hash_join"] == "false"
    assert settings["datafusion.optimizer.enable_round_robin_repartition"] == "false"


def test_restricted_options_override_dict():
    key = "datafusion.optimizer.prefer_hash_join"
    assert _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS[key]

    settings = _settings(BallistaSessionConfig({key: True}).build())

    assert settings[key] == "false"


def test_round_robin_repartition_disabled_without_config():
    settings = _settings(BallistaSessionConfig().build())

    assert settings["datafusion.optimizer.enable_round_robin_repartition"] == "false"


def test_non_restricted_options_from_session_config():
    config = SessionConfig().with_target_partitions(3).with_batch_size(1234)
    settings = _settings(BallistaSessionConfig(config).build())

    assert settings["datafusion.execution.target_partitions"] == "16"
    assert settings["datafusion.execution.batch_size"] == "1234"


def test_target_partitions_default():
    settings = _settings(BallistaSessionConfig().build())

    assert settings["datafusion.execution.target_partitions"] == "16"


def test_target_partitions_overridden_by_dict():
    config = {"datafusion.execution.target_partitions": 128}
    settings = _settings(BallistaSessionConfig(config).build())

    assert settings["datafusion.execution.target_partitions"] == "128"


def test_target_partitions_overridden_by_set_on_session_config():
    config = SessionConfig().with_batch_size(1234)
    settings = _settings(
        BallistaSessionConfig(config)
        .set("datafusion.execution.target_partitions", 128)
        .build()
    )

    assert settings["datafusion.execution.target_partitions"] == "128"
    assert settings["datafusion.execution.batch_size"] == "1234"


def test_dict_keeps_non_restricted_options():
    key = "datafusion.sql_parser.map_string_types_to_utf8view"
    assert not _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS[key]
    assert ballista_datafusion_config_defaults()[key] == "false"

    settings = _settings(BallistaSessionConfig({key: True}).build())

    assert settings[key] == "true"


def test_session_config_overrides_non_restricted_options():
    key = "datafusion.sql_parser.map_string_types_to_utf8view"
    config = SessionConfig().set(key, "true")

    settings = _settings(BallistaSessionConfig(config).build())

    assert settings[key] == "false"


def test_ballista_defaults_set_without_config():
    expected = _settings(SessionConfig().with_extension(BallistaConfigExtension()))
    settings = _settings(BallistaSessionConfig().build())

    ballista_defaults = {k: v for k, v in expected.items() if k.startswith("ballista.")}
    assert ballista_defaults
    for key, value in ballista_defaults.items():
        assert settings[key] == value, key


def test_dict_accepts_ballista_and_datafusion_options():
    settings = _settings(
        BallistaSessionConfig(
            {
                "datafusion.execution.batch_size": 1234,
                "ballista.job.name": "test_job",
                "ballista.shuffle.force_remote_read": True,
            }
        ).build()
    )

    assert settings["datafusion.execution.batch_size"] == "1234"
    assert settings["ballista.job.name"] == "test_job"
    assert settings["ballista.shuffle.force_remote_read"] == "true"


def test_set_overrides_ballista_defaults():
    key = "ballista.shuffle.max_concurrent_read_requests"
    assert _settings(BallistaSessionConfig().build())[key] != "7"

    settings = _settings(BallistaSessionConfig().set(key, 7).build())

    assert settings[key] == "7"


@pytest.mark.parametrize(
    "method, value, key, expected",
    [
        ("with_job_name", "test_job", "ballista.job.name", "test_job"),
        (
            "with_shuffle_reader_maximum_concurrent_requests",
            7,
            "ballista.shuffle.max_concurrent_read_requests",
            "7",
        ),
        (
            "with_shuffle_reader_force_remote_read",
            True,
            "ballista.shuffle.force_remote_read",
            "true",
        ),
        (
            "with_adaptive_query_planner",
            True,
            "ballista.planner.adaptive.enabled",
            "true",
        ),
        (
            "with_broadcast_join_threshold_bytes",
            1024,
            "ballista.optimizer.broadcast_join_threshold_bytes",
            "1024",
        ),
        (
            "with_coalesce_small_partition_factor",
            0.5,
            "ballista.planner.coalesce.small_partition_factor",
            "0.5",
        ),
        (
            "with_shuffle_compression_codec",
            "zstd",
            "ballista.shuffle.compression.codec",
            "zstd",
        ),
        (
            "with_grpc_client_http2_keepalive_interval_seconds",
            42,
            "ballista.client.grpc_http2_keepalive_interval_seconds",
            "42",
        ),
        (
            "with_share_file_statistics_cache",
            False,
            "ballista.scheduler.share_file_statistics_cache",
            "false",
        ),
    ],
)
def test_typed_setters(method, value, key, expected):
    config = BallistaSessionConfig()
    assert getattr(config, method)(value) is config

    settings = _settings(config.build())

    assert settings[key] == expected


def test_setters_chain():
    settings = _settings(
        BallistaSessionConfig()
        .with_job_name("test_job")
        .with_use_tls(True)
        .with_client_pull(True)
        .set("ballista.testing.chaos_execution.probability", 0.25)
        .build()
    )

    assert settings["ballista.job.name"] == "test_job"
    assert settings["ballista.client.use_tls"] == "true"
    assert settings["ballista.client.pull"] == "true"
    assert settings["ballista.testing.chaos_execution.probability"] == "0.25"


def test_set_rejects_unsupported_prefix():
    with pytest.raises(ValueError):
        BallistaSessionConfig().set("unknown.option", "1")


def test_set_rejects_unknown_ballista_option():
    with pytest.raises(ValueError):
        BallistaSessionConfig().set("ballista.unknown.option", "1")


def test_dict_rejects_unsupported_prefix():
    with pytest.raises(ValueError):
        BallistaSessionConfig({"unknown.option": "1"})


def test_build_is_repeatable():
    config = BallistaSessionConfig().with_job_name("test_job")

    first = _settings(config.build())
    second = _settings(config.build())

    assert first == second
    assert second["ballista.job.name"] == "test_job"


_CLIENT_OPTIONS = ("ballista.client.use_tls", "ballista.client.pull")

_USER_CONFIGS = pytest.mark.parametrize(
    "make_config",
    [
        lambda: None,
        lambda: SessionConfig().with_target_partitions(3),
        lambda: {"datafusion.execution.target_partitions": "3"},
    ],
    ids=["no_config", "session_config", "dict_config"],
)


@_USER_CONFIGS
def test_client_options_default(make_config):
    settings = _settings(BallistaSessionConfig(make_config()).build())

    for key in _CLIENT_OPTIONS:
        assert settings[key] == "false", key


@_USER_CONFIGS
def test_client_options_set(make_config):
    config = BallistaSessionConfig(make_config())
    for key in _CLIENT_OPTIONS:
        config.set(key, True)
    settings = _settings(config.build())

    for key in _CLIENT_OPTIONS:
        assert settings[key] == "true", key
