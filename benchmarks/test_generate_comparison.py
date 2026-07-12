# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import importlib.util
import os
import unittest

_HERE = os.path.dirname(__file__)
_spec = importlib.util.spec_from_file_location(
    "generate_comparison", os.path.join(_HERE, "generate-comparison.py")
)
gc = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(gc)


class NormalizeResultTest(unittest.TestCase):
    def test_ballista_format_is_converted(self):
        raw = {
            "queries": [
                {
                    "query": 1,
                    "iterations": [
                        {"elapsed": 0.5, "row_count": 4},
                        {"elapsed": 0.7, "row_count": 4},
                    ],
                },
                {"query": 2, "iterations": [{"elapsed": 1.25, "row_count": 100}]},
            ]
        }
        out = gc.normalize_result(raw)
        self.assertEqual(out["1"]["durations"], [0.5, 0.7])
        self.assertEqual(out["1"]["row_count"], 4)
        self.assertEqual(out["2"]["durations"], [1.25])
        self.assertEqual(out["2"]["row_count"], 100)

    def test_comet_format_passes_through(self):
        raw = {
            "data_path": "s3a://tpch/sf100",
            "1": {"durations": [1.1, 1.2], "row_count": 4, "result_hash": "abc"},
        }
        out = gc.normalize_result(raw)
        self.assertIs(out, raw)


if __name__ == "__main__":
    unittest.main()
