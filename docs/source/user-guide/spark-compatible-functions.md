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

# Spark-Compatible Functions

Ballista provides an optional `spark-compat` Cargo feature that enables Spark-compatible scalar, aggregate, and window functions from the [datafusion-spark](https://crates.io/crates/datafusion-spark) crate.

## Enabling the Feature

The `spark-compat` feature must be explicitly enabled at build time. It is _not_ enabled by default.

### Building from Source

To build Ballista components with Spark-compatible functions:

```bash
# Build all components with spark-compat feature
cargo build --features spark-compat --release

# Build scheduler only
cargo build -p ballista-scheduler --features spark-compat --release

# Build executor only
cargo build -p ballista-executor --features spark-compat --release

# Build CLI with spark-compat
cargo build -p ballista-cli --features spark-compat --release
```

For more installation options, see [Installing with Cargo](deployment/cargo-install.md).

## What's Included

When the `spark-compat` feature is enabled, Ballista's function registry automatically includes additional functions from the `datafusion-spark` crate:

> **Note:** For a comprehensive list of available functions, refer to the [datafusion-spark crate documentation](https://docs.rs/datafusion-spark/latest/datafusion_spark/). These functions are provided in addition to DataFusion's default functions.

### Scalar Functions

Spark-compatible scalar functions provide additional string, mathematical, and cryptographic operations.

### Aggregate Functions

Spark-compatible aggregate functions extend DataFusion's built-in aggregations with additional statistical and analytical functions.

### Window Functions

Spark-compatible window functions provide additional analytical capabilities for windowed operations.

## Usage Examples

Once the `spark-compat` feature is enabled at build time, the functions are automatically available in SQL queries:

### Example 1: Using SHA-1 Hash Function

```sql
SELECT sha1('Ballista') AS hash_value;
```

Output:

```
+------------------------------------------+
| hash_value                               |
+------------------------------------------+
| 8b8e1f0e55f8f0e3c7a8... (hex string)    |
+------------------------------------------+
```

### Example 2: Using expm1 for Precision

```sql
SELECT
    expm1(0.001) AS precise_value,
    exp(0.001) - 1 AS standard_value;
```

The `expm1` function provides better numerical precision for small values compared to computing `exp(x) - 1` directly.

### Example 3: Combining with DataFusion Functions

Spark-compatible functions work alongside DataFusion's built-in functions:

```sql
SELECT
    name,
    upper(name) AS name_upper,           -- DataFusion function
    sha1(name) AS name_hash,             -- Spark-compat function
    length(name) AS name_length          -- DataFusion function
FROM users;
```

## Use Cases

The `spark-compat` feature is useful when:

- **Migrating from Spark**: Easing the transition by providing familiar function names and behaviors
- **Cross-Platform Queries**: Writing queries that use similar functions across Spark and Ballista environments
- **Specific Function Needs**: Requiring particular Spark-style functions (like `sha1`, `conv`, etc.) that aren't in DataFusion's default set
- **Team Familiarity**: Your team is more familiar with Spark's function library

## See Also

- [datafusion-spark crate](https://crates.io/crates/datafusion-spark) - Source of the Spark-compatible functions
- [Installing with Cargo](deployment/cargo-install.md) - Detailed installation instructions
