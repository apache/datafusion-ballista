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
    _produce_valid_config,
)
from datafusion import SessionConfig, SessionContext


def _settings(config: SessionConfig) -> dict:
    ctx = SessionContext(config)
    ctx.sql("SET datafusion.catalog.information_schema = true").collect()
    rows = ctx.sql("SELECT name, value FROM information_schema.df_settings").to_pylist()
    return {row["name"]: row["value"] for row in rows}


def test_restricted_options_set_without_config():
    defaults = ballista_datafusion_config_defaults()
    settings = _settings(_produce_valid_config())

    for key in _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS:
        assert settings[key] == defaults[key], key


def test_restricted_options_override_user_config():
    config = SessionConfig().set("datafusion.optimizer.prefer_hash_join", "true")
    settings = _settings(_produce_valid_config(config))

    assert settings["datafusion.optimizer.prefer_hash_join"] == "false"


def test_round_robin_repartition_disabled_without_config():
    settings = _settings(_produce_valid_config())

    assert settings["datafusion.optimizer.enable_round_robin_repartition"] == "false"


def test_round_robin_repartition_disabled_with_user_config():
    config = SessionConfig().set(
        "datafusion.optimizer.enable_round_robin_repartition", "true"
    )
    settings = _settings(_produce_valid_config(config))

    assert settings["datafusion.optimizer.enable_round_robin_repartition"] == "false"


def test_non_restricted_options_kept_from_user_config():
    config = SessionConfig().with_target_partitions(3).with_batch_size(1234)
    settings = _settings(_produce_valid_config(config))

    assert settings["datafusion.execution.target_partitions"] == "16"
    assert settings["datafusion.execution.batch_size"] == "1234"


def test_target_partitions_default():
    settings = _settings(_produce_valid_config())

    assert settings["datafusion.execution.target_partitions"] == "16"


def test_target_partitions_overridden_by_user_config():
    config = {"datafusion.execution.target_partitions": "128"}
    settings = _settings(_produce_valid_config(config))

    assert settings["datafusion.execution.target_partitions"] == "128"


def test_ballista_defaults_set_without_cluster_config():
    expected = _settings(SessionConfig().with_extension(BallistaConfigExtension()))
    settings = _settings(_produce_valid_config())

    ballista_defaults = {k: v for k, v in expected.items() if k.startswith("ballista.")}
    assert ballista_defaults
    for key, value in ballista_defaults.items():
        assert settings[key] == value, key


def test_cluster_config_overrides_ballista_defaults():
    key = "ballista.shuffle.max_concurrent_read_requests"
    assert _settings(_produce_valid_config())[key] != "7"

    settings = _settings(
        _produce_valid_config(cluster_config={"ballista.job.name": "test_job", key: 7})
    )

    assert settings["ballista.job.name"] == "test_job"
    assert settings[key] == "7"


def test_cluster_config_ignores_non_ballista_options():
    settings = _settings(
        _produce_valid_config(
            cluster_config={"datafusion.execution.batch_size": "1234"}
        )
    )

    assert settings["datafusion.execution.batch_size"] != "1234"


def test_cluster_config_accepts_bool_values():
    key = "ballista.shuffle.force_remote_read"
    settings = _settings(_produce_valid_config(cluster_config={key: True}))

    assert settings[key] == "true"


def test_config_accepts_dict():
    config = {
        "datafusion.execution.target_partitions": "3",
        "datafusion.optimizer.enable_round_robin_repartition": "true",
    }
    settings = _settings(_produce_valid_config(config))

    assert settings["datafusion.execution.target_partitions"] == "3"
    assert settings["datafusion.optimizer.enable_round_robin_repartition"] == "false"


def test_config_dict_keeps_non_restricted_options():
    key = "datafusion.sql_parser.map_string_types_to_utf8view"
    assert not _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS[key]
    assert ballista_datafusion_config_defaults()[key] == "false"

    settings = _settings(_produce_valid_config({key: "true"}))

    assert settings[key] == "true"


def test_config_dict_overrides_restricted_options():
    key = "datafusion.optimizer.prefer_hash_join"
    assert _BALLISTA_RESTRICTED_DATAFUSION_OPTIONS[key]

    settings = _settings(_produce_valid_config({key: "true"}))

    assert settings[key] == "false"


def test_session_config_overrides_non_restricted_options():
    key = "datafusion.sql_parser.map_string_types_to_utf8view"
    config = SessionConfig().set(key, "true")

    settings = _settings(_produce_valid_config(config))

    assert settings[key] == "false"


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
    settings = _settings(_produce_valid_config(make_config()))

    for key in _CLIENT_OPTIONS:
        assert settings[key] == "false", key


@_USER_CONFIGS
def test_client_options_set_from_cluster_config(make_config):
    cluster_config = {key: True for key in _CLIENT_OPTIONS}
    settings = _settings(_produce_valid_config(make_config(), cluster_config))

    for key in _CLIENT_OPTIONS:
        assert settings[key] == "true", key
