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

# Querying S3 Data

Ballista supports querying data stored in Amazon S3. The default scheduler and executor
binaries include S3 support out of the box.

## Prerequisites

- A running Ballista cluster (scheduler + executors)
- `AWS_REGION` environment variable set on the scheduler and executor processes
- AWS credentials available via the standard credential chain (environment variables,
  instance profiles, IAM roles for service accounts, etc.)

## Environment Variables

The scheduler and executor processes need AWS configuration to access S3. Set the
following environment variables on **both** the scheduler and executor:

| Variable             | Description                               | Example     |
| -------------------- | ----------------------------------------- | ----------- |
| `AWS_REGION`         | AWS region for S3 requests                | `us-west-2` |
| `AWS_DEFAULT_REGION` | Fallback region (recommended to set both) | `us-west-2` |

For authentication, the standard AWS credential chain is used. On EKS, attach an IAM
role to your service account. For local development, set:

| Variable                | Description                                    |
| ----------------------- | ---------------------------------------------- |
| `AWS_ACCESS_KEY_ID`     | AWS access key                                 |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key                                 |
| `AWS_SESSION_TOKEN`     | Session token (if using temporary credentials) |

## Registering an S3 Object Store

Before querying S3 data, register an S3 object store on the client context. This allows
the client to read Parquet metadata for schema inference during table registration.

```python
from ballista import BallistaSessionContext
from datafusion.object_store import AmazonS3

ctx = BallistaSessionContext("df://localhost:50050")

# IMPORTANT: The scheme parameter must include "://" — DataFusion concatenates
# scheme + host directly, so "s3" alone would produce an invalid URL.
ctx.register_object_store("s3://", AmazonS3(
    bucket_name="my-bucket",
    region="us-west-2",
))
```

> **Note:** `AmazonS3` also reads `AWS_REGION`, `AWS_ACCESS_KEY_ID`, and
> `AWS_SECRET_ACCESS_KEY` from environment variables, so you can omit the `region`
> parameter if the environment is already configured.

## Creating External Tables

Once the object store is registered, create external tables pointing to your S3 data:

```python
ctx.sql("""
    CREATE EXTERNAL TABLE lineitem
    STORED AS PARQUET
    LOCATION 's3://my-bucket/tpch/lineitem/'
""")
```

## Running Queries

After registering tables, run queries as usual. The client sends the query plan to the
Ballista scheduler, which distributes execution across executors:

```python
# Verify table registration
ctx.sql("SHOW TABLES").show()

# Inspect table schema
ctx.sql("DESCRIBE lineitem").show()

# Run a query
df = ctx.sql("""
    SELECT l_returnflag, l_linestatus,
           SUM(l_quantity) as sum_qty,
           SUM(l_extendedprice) as sum_base_price
    FROM lineitem
    WHERE l_shipdate <= '1998-09-02'
    GROUP BY l_returnflag, l_linestatus
    ORDER BY l_returnflag, l_linestatus
""")
df.show()
```

## Complete Example

```python
import os
from ballista import BallistaSessionContext
from datafusion.object_store import AmazonS3

os.environ.setdefault("AWS_REGION", "us-west-2")

ctx = BallistaSessionContext("df://localhost:50050")

ctx.register_object_store("s3://", AmazonS3(
    bucket_name="my-data-bucket",
    region="us-west-2",
))

tables = ["lineitem", "orders", "customer", "nation", "region",
          "part", "supplier", "partsupp"]

for table in tables:
    ctx.sql(f"""
        CREATE EXTERNAL TABLE {table}
        STORED AS PARQUET
        LOCATION 's3://my-data-bucket/tpch/{table}/'
    """)

ctx.sql("SHOW TABLES").show()

df = ctx.sql("SELECT count(*) FROM lineitem")
df.show()
```

## Configuring S3 via SQL

Ballista also supports configuring S3 credentials and endpoints through SQL `SET`
commands. This configures the **scheduler and executor** session state and propagates
across the cluster:

```sql
SET s3.region = 'us-west-2';
SET s3.access_key_id = '******';
SET s3.secret_access_key = '******';
SET s3.endpoint = 'https://s3.us-west-2.amazonaws.com';
SET s3.allow_http = false;
```

> **Note:** These `SET` commands are separate from the client-side
> `register_object_store()` call, which is still needed for local schema inference
> during `CREATE EXTERNAL TABLE`.

## Kubernetes Deployment

When deploying on Kubernetes, set AWS environment variables in your pod specs:

```yaml
# scheduler and executor containers
env:
  - name: AWS_REGION
    value: "us-west-2"
  - name: AWS_DEFAULT_REGION
    value: "us-west-2"
```

On EKS with [IAM Roles for Service Accounts (IRSA)](https://docs.aws.amazon.com/eks/latest/userguide/iam-roles-for-service-accounts.html),
attach an IAM role with S3 permissions to your Kubernetes service account. The AWS SDK
credential chain will automatically pick up the projected token.

## Troubleshooting

### "No suitable object store found for s3://..."

The client context does not have an S3 object store registered. Call
`register_object_store()` before creating external tables:

```python
from datafusion.object_store import AmazonS3
ctx.register_object_store("s3://", AmazonS3(bucket_name="my-bucket"))
```

### "RelativeUrlWithoutBase" panic

The `scheme` parameter in `register_object_store()` must include `://`:

```python
# Wrong — produces invalid URL "s3my-bucket"
ctx.register_object_store("s3", store)

# Correct
ctx.register_object_store("s3://", store)
```

### S3 access works on client but queries fail on executors

The scheduler and executor processes need AWS credentials and region configuration
independently of the client. Ensure `AWS_REGION` is set on all processes, and that
credentials are available via environment variables, instance profiles, or IRSA.
