<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Iceberg build gate

Enable `ballista-core/contrib-iceberg` to use the SDK-free Iceberg codec gate. The integration supplies the Iceberg SDK, catalogs and table reconstruction.

Implement `IcebergTableProviderCodec` and `IcebergScanCodec`, then wrap the session's existing codecs with `IcebergLogicalCodec<T>` and `IcebergPhysicalCodec<T>`. Return an unframed payload for owned objects, `None` for other objects, and errors for unsupported or invalid owned objects. Install compatible adapters on every participating client, scheduler and executor.

The gate frames payloads with `ballista-contrib-iceberg-v1\0` and forwards unhandled operations to the inner codec. The integration owns payload compatibility.

Keep the integration in the excluded `integrations/iceberg` directory or a separate repository, with its own workspace, SDK dependencies, lockfile and CI. Released integrations should select a compatible released Ballista version; local path dependencies are for joint development. This allows independent release schedules, but does not bridge incompatible DataFusion or Arrow types.

Check the gate without an Iceberg SDK:

```shell
cargo check -p ballista-core --no-default-features --features contrib-iceberg --locked
```
