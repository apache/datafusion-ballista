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

// filter pushdown has been copied over
// from datafusion to patch non idempotent
// behavior.
// TODO: remove when updated to datafusion 55
pub mod filter_pushdown;
// join selection has been copied over from
// datafusion and patched to support ballista
// specific cases. it has been used in static
// execution graph only.
pub mod join_selection;
// output requirements has been copied over
// from datafusion to patch non idempotent
// behavior.
// TODO: remove when updated to datafusion 55
pub mod output_requirements;
