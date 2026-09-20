# Revivial of the excellent Kafka Connect Iceberg Sink 

Compiled and test (ing atm) for:
* Kafka 3.9.1
* Iceberg 1.10.0
* Parquet 1.16.0
* Debezium 3.1.3 FINAL  (Last one aligned for kafka 3.9.1)
  
---

# ⚠ FORK ADDITIONS — branch `bench_fix` (NOT in upstream getindata)

Everything between this heading and the line *BELOW IS FROM THE ORIGINAL README THAT IS FROZEN*
is added in this fork. The upstream connector is archived; these additions are ours:

1. **Type mapping** — every Debezium / Connect logical type becomes its Iceberg type
   (decimal, date, time, timestamp, uuid, map, nested lists).
2. **Namespace auto-create** — no more `NoSuchNamespaceException` on the first table.
3. **Per-table and per-column settings** — `table.rule.<id>.*`: upsert / append, identifier
   columns, sort order, partitioning, table properties per table in ONE connector.
4. **Flush buffer** — Iceberg file size set in bytes and records, not by Kafka poll size;
   BLOB columns bypass the JSON rendering.
5. **`table.namespace.nuke`** — start over in the lake without touching Kafka.

All of it is backwards compatible: with an unchanged configuration the connector behaves as
before, plus the type mapping and the namespace creation. All examples below use made-up names.

## 1. Type mapping

Every Debezium / Kafka Connect logical type is now mapped to its Iceberg type (format v2,
Iceberg 1.10). `rich-temporal-types` is deprecated and ignored: the mapping is always on.

| Connect schema `name` | Connect `type` | Iceberg type | note |
|---|---|---|---|
| `org.apache.kafka.connect.data.Decimal` | bytes (base64 unscaled) | `decimal(p,s)` | `p` from `connect.decimal.precision` (38 if absent, clamped to 38), `s` from `scale` |
| `io.debezium.data.VariableScaleDecimal` | struct{scale,value} | `string` (default) or `decimal(38,N)` | `types.variable-scale-decimal = string \| N` |
| `io.debezium.time.Date`, `…connect.data.Date` | int32 days | `date` | |
| `io.debezium.time.Time`, `…connect.data.Time` | int32 ms | `time` | |
| `io.debezium.time.MicroTime` / `NanoTime` | int64 µs / ns | `time` | ns truncated to µs |
| `io.debezium.time.ZonedTime` | string | `time` | Debezium already normalised to UTC |
| `io.debezium.time.Timestamp`, `…connect.data.Timestamp` | int64 ms | `timestamp` | |
| `io.debezium.time.MicroTimestamp` / `NanoTimestamp` | int64 µs / ns | `timestamp` | ns truncated to µs |
| `io.debezium.time.ZonedTimestamp` | string ISO-8601 | `timestamptz` | |
| `io.debezium.data.Uuid` | string | `uuid` | `types.uuid-as-string=true` keeps `string` |
| `io.debezium.data.Bits` | bytes | `fixed(ceil(length/8))` | `binary` when `length` is absent |
| `map` (primitive keys) | object / [[k,v],…] | `map<k,v>` | was an exception |
| `array` of `array` / `map` / logical | | `list<…>` | was an exception |
| anything else | | as before (`string` fallback) | |

The Connect logical name is stored as the Iceberg column `doc`, that is how the writer knows
the source unit (ms / µs / ns) for an existing table. Existing tables created by the old
mapping keep their old types: Iceberg does not promote `binary → decimal` or
`long → timestamp`, recreate those tables.

## 2. Namespace auto-create

`table.namespace.auto-create` (default `true`): with `table.auto-create=true` the namespace
is created in the catalog (Nessie, REST, Hive, …) if it does not exist, instead of failing the
first table create.

## 3. Per-table and per-column settings (`table.rule.<id>.*`)

One connector, different semantics per table. A rule is a regex over the table name (prefix
included, namespace excluded); the first matching rule in `table.rules` order wins, unmatched
tables get the connector defaults.

```properties
table.rules=events,dims
# big event / transaction tables: append, day partitioned, sorted for range scans
table.rule.events.regex=^cdc_shop_(order|invoice|event)
table.rule.events.upsert=false
table.rule.events.partition=day(__source_ts)
table.rule.events.sort-order=order_id asc,line_no asc
table.rule.events.iceberg.table-default.write.parquet.compression-codec=zstd
# small dimension tables: current picture, bucketed on the key so upsert + partitioning is safe
table.rule.dims.regex=^cdc_shop_dim_
table.rule.dims.upsert=true
table.rule.dims.identifier-columns=id
table.rule.dims.partition=bucket(id,16)
```

| Rule setting | Meaning |
|---|---|
| `regex` | required, matched with `find()` against the table name |
| `upsert`, `upsert.keep-deletes` | per-table write mode |
| `identifier-columns` | the row identity ("id columns"); default = the Debezium key columns. Also the default sort order and the equality-delete columns |
| `sort-order` | `col [asc\|desc], …`; default = identifier columns ascending |
| `partition` | `identity(col)` / `col`, `year|month|day|hour(col)`, `bucket(col,N)`, `truncate(col,W)`; default = `day(partition.column)` in append mode, unpartitioned in upsert mode |
| `partition.column`, `partition.timestamp` | per-table override of `iceberg.partition.*` |
| `iceberg.table-default.*` | table properties merged over the global ones |

Bug fixed on the way: upstream stripped `iceberg.table-default` without the dot, so every
global `iceberg.table-default.x` became a table property named `.x` and never took effect.

Upsert tables MAY be partitioned, but only on values that never change for a row (an identifier
column, `bucket(id,N)`, a business date): Iceberg equality deletes are partition scoped, so a
row whose partition value changes would keep its old version. Partitioning an upsert table on
`__source_ts` is logged as a warning.

Partitioning, sort order and identifier columns are fixed at table create. A rule change only
affects tables created after it.

## 4. Flush buffer: file size in bytes, not in polls

Records are buffered across Kafka polls and written to Iceberg (one commit per table) when
`flush.size` records or `flush.max-bytes` bytes are buffered, or when Connect commits offsets
(`offset.flush.interval.ms`, so a rebalance never loses rows). One Iceberg commit per poll is gone:
`consumer.override.max.poll.records` can be high for small rows while `flush.max-bytes` bounds the
heap for big ones (a 200 MB row flushes alone).

| Key | Default | Meaning |
|---|---|---|
| `flush.size` | 10000 | records per flush (upper bound) |
| `flush.max-bytes` | 268435456 | estimated payload bytes per flush (upper bound) |
| `binary.bypass-json` | true | plain bytes columns (BLOBs) go straight from the Connect record into Iceberg, never through base64 JSON; ~4× less heap per BLOB row |

Recommended consumer overrides with this: `consumer.override.max.poll.records` high (thousands),
`consumer.override.fetch.max.bytes` around 64 MB as the per-poll memory guard (one message larger
than that is still delivered, alone).

## 5. `table.namespace.nuke=<token>` — start over in the lake, keep Kafka

DESTRUCTIVE. At connector start (once, before any task runs) every table whose name starts with
`table.prefix` in `table.namespace` is dropped from the catalog and its files are deleted under
the table location in the object store (Nessie ignores purge, so the sink deletes through the
table's FileIO). Kafka topics and consumer offsets are not touched: reset the consumer group or
rename the connector to replay. The token is stored as a namespace property; the same token does
not nuke again on restart, a new token does. Tables without the prefix are kept.

## 6. Verifying a configuration (for whoever reviews the settings)

Before the flow is started, with `table.rule.*` in place:

1. **Config validation** — `PUT /connector-plugins/com.getindata.kafka.connect.iceberg.sink.IcebergSink/config/validate`
   with the connector JSON. Bad rules (missing regex, unknown key, bad boolean, undeclared id
   in `table.rules`) fail here, before anything is written.
2. **Which rule hits which table** — every table create logs
   `Table '<ns>.<name>' settings: TableSettings{rule='<id>', upsert=…, partition=[…], …}`.
   Grep the connect log for `settings: TableSettings` and check every table got the intended rule
   (`rule=''` means connector defaults).
3. **The shape in Iceberg** (Spark SQL against the catalog):
   `DESCRIBE EXTENDED <ns>.<table>` → *Partitioning* (expect `days(__source_ts)` for append
   rules, `bucket(N, id)` or none for upsert), *Sort Order*, and the identifier columns under
   *Table Properties* / `schema.identifier-field-ids`.
   Column types: `DESCRIBE <ns>.<table>` → `decimal(p,s)`, `timestamp`, `date`, `uuid`, never
   `binary` for an amount and never `bigint` for a timestamp.
4. **Commit cadence** — `SELECT count(*) FROM <ns>.<table>.snapshots` should grow by one per
   flush, not per Kafka poll. The connect log line
   `Flushed N records (~B bytes) buffered for T ms, trigger: size|offset commit` shows what
   triggered each flush.
5. **Memory** — heap stays flat around `flush.max-bytes` × 2 while a BLOB-heavy topic streams;
   with `binary.bypass-json=false` it would spike to ~4× the BLOB per row.
6. **Nuke was one-shot** — `SELECT * FROM <catalog>.<ns>` namespace properties (or Nessie UI)
   shows `kafka-connect-iceberg-sink.nuke-token=<token>`; a restart with the same token logs
   `NUKE token '…' already applied … skipping`.

---

**BELOW IS FROM THE ORIGINAL README THAT IS FROZEN** (upstream getindata, unchanged except the
configuration reference, which lists the fork's keys at the end)


# Kafka Connect Iceberg Sink

This repository is archived. There is official support for Kafka Connect in Apache Iceberg project https://iceberg.apache.org/docs/latest/kafka-connect/

Based on https://github.com/memiiso/debezium-server-iceberg

## Build

```shell
mvn clean package
```

## Usage

### Configuration reference

| Key                         | Type    | Default value    | Description                                                                                                                                                 |
| --------------------------- | ------- | ---------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| upsert                      | boolean | true             | When _true_ Iceberg rows will be updated based on table primary key. When _false_ all modification will be added as separate rows.                          |
| upsert.keep-deletes         | boolean | true             | When _true_ delete operation will leave a tombstone that will have only a primary key and \*\_\_deleted\*\* flag set to true                                |
| upsert.dedup-column         | String  | \_\_source_ts_ms | Column used to check which state is newer during upsert                                                                                                     |
| upsert.op-column            | String  | \_\_op           | Column used to check which state is newer during upsert when _upsert.dedup-column_ is not enough to resolve                                                 |
| allow-field-addition        | boolean | true             | When _true_ sink will be adding new columns to Iceberg tables on schema changes                                                                             |
| table.auto-create           | boolean | false            | When _true_ sink will automatically create new Iceberg tables                                                                                               |
| table.namespace             | String  | default          | Table namespace. In Glue it will be used as database name                                                                                                   |
| table.prefix                | String  | _empty string_   | Prefix added to all table names                                                                                                                             |
| iceberg.name                | String  | default          | Iceberg catalog name                                                                                                                                        |
| iceberg.catalog-impl        | String  | _null_           | Iceberg catalog implementation (Only one of iceberg.catalog-impl and iceberg.type can be set to non null value at the same time                             |
| iceberg.type                | String  | _null_           | Iceberg catalog type (Only one of iceberg.catalog-impl and iceberg.type can be set to non null value at the same time)                                      |
| iceberg.\*                  |         |                  | All properties with this prefix will be passed to Iceberg Catalog implementation                                                                            |
| iceberg.table-default.\*    |         |                  | Iceberg specific table settings can be changed with this prefix, e.g. 'iceberg.table-default.write.format.default' can be set to 'orc'                      |
| iceberg.partition.column    | String  | \_\_source_ts    | Column used for partitioning. If the column already exists, it must be of type timestamp.                                                                   |
| iceberg.partition.timestamp | String  | \_\_source_ts_ms | Column containing unix millisecond timestamps to be converted to partitioning times. If equal to partition.column, values will be replaced with timestamps. |
| iceberg.format-version      | String  | 2                | Specification for the Iceberg table format. Version 1: Analytic Data Tables. Version 2: Row-level Deletes. Default 2.                                       |
| table.namespace.auto-create | boolean | true             | Create the namespace in the catalog if it does not exist (with table.auto-create)                                                                           |
| types.uuid-as-string        | boolean | false            | Store io.debezium.data.Uuid as string instead of Iceberg uuid                                                                                               |
| types.variable-scale-decimal| String  | string           | io.debezium.data.VariableScaleDecimal as `string` (lossless) or an integer scale N → decimal(38,N)                                                           |
| rich-temporal-types         | boolean | true             | Deprecated, ignored: logical types are always mapped                                                                                                        |
| table.rules / table.rule.<id>.* |     |                  | Per-table rules, see above                                                                                                                                  |
| flush.size                  | int     | 10000            | Records per flush                                                                                                                                           |
| flush.max-bytes             | long    | 268435456        | Bytes per flush                                                                                                                                             |
| binary.bypass-json          | boolean | true             | BLOB columns bypass the JSON rendering                                                                                                                      |
| table.namespace.nuke        | String  | _empty_          | DESTRUCTIVE token: drop + delete all prefixed tables in the namespace at start, once per token                                                             |

### REST / Manual based installation

1. Copy content of `kafka-connect-iceberg-sink-0.1.4-SNAPSHOT-plugin.zip` into Kafka Connect plugins directory. [Kafka Connect installing plugins](https://docs.confluent.io/home/connect/self-managed/userguide.html#connect-installing-plugins)

2. POST `<kafka_connect_host>:<kafka_connect_port>/connectors`

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "topics": "topic1,topic2",

    "upsert": true,
    "upsert.keep-deletes": true,

    "table.auto-create": true,
    "table.write-format": "parquet",
    "table.namespace": "my_namespace",
    "table.prefix": "debeziumcdc_",

    "iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    "iceberg.fs.defaultFS": "s3a://my_bucket/iceberg",
    "iceberg.com.amazonaws.services.s3.enableV4": true,
    "iceberg.com.amazonaws.services.s3a.enableV4": true,
    "iceberg.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    "iceberg.fs.s3a.path.style.access": true,
    "iceberg.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "iceberg.fs.s3a.access.key": "my-aws-access-key",
    "iceberg.fs.s3a.secret.key": "my-secret-access-key"
  }
}
```

### Running with debezium/connect docker image

```shell
docker run -it --name connect --net=host -p 8083:8083 \
  -e GROUP_ID=1 \
  -e CONFIG_STORAGE_TOPIC=my-connect-configs \
  -e OFFSET_STORAGE_TOPIC=my-connect-offsets \
  -e BOOTSTRAP_SERVERS=localhost:9092 \
  -e CONNECT_TOPIC_CREATION_ENABLE=true \
  -v ~/.aws/config:/kafka/.aws/config \
  -v ./target/plugin/kafka-connect-iceberg-sink:/kafka/connect/kafka-connect-iceberg-sink \
  debezium/connect
```

### Strimzi

KafkaConnect:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnect
metadata:
  name: my-connect-cluster
  annotations:
    strimzi.io/use-connector-resources: "true"
spec:
  version: 3.3.1
  replicas: 1
  bootstrapServers: kafka-cluster-kafka-bootstrap:9093
  tls:
    trustedCertificates:
      - secretName: kafka-cluster-cluster-ca-cert
        certificate: ca.crt
  logging:
    type: inline
    loggers:
      log4j.rootLogger: "INFO"
      log4j.logger.com.getindata.kafka.connect.iceberg.sink.IcebergSinkTask: "DEBUG"
      log4j.logger.org.apache.hadoop.io.compress.CodecPool: "WARN"
  metricsConfig:
    type: jmxPrometheusExporter
    valueFrom:
      configMapKeyRef:
        name: connect-metrics
        key: metrics-config.yml
  config:
    group.id: my-connect-cluster
    offset.storage.topic: my-connect-cluster-offsets
    config.storage.topic: my-connect-cluster-configs
    status.storage.topic: my-connect-cluster-status
    # -1 means it will use the default replication factor configured in the broker
    config.storage.replication.factor: -1
    offset.storage.replication.factor: -1
    status.storage.replication.factor: -1
    config.providers: file,secret,configmap
    config.providers.file.class: org.apache.kafka.common.config.provider.FileConfigProvider
    config.providers.secret.class: io.strimzi.kafka.KubernetesSecretConfigProvider
    config.providers.configmap.class: io.strimzi.kafka.KubernetesConfigMapConfigProvider
  build:
    output:
      type: docker
      image: <yourdockerregistry>
      pushSecret: <yourpushSecret>
    plugins:
      - name: debezium-postgresql
        artifacts:
          - type: zip
            url: https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/2.0.0.Final/debezium-connector-postgres-2.0.0.Final-plugin.zip
      - name: iceberg
        artifacts:
          - type: zip
            url: https://github.com/TIKI-Institut/kafka-connect-iceberg-sink/releases/download/0.1.4-SNAPSHOT-hadoop-catalog-r3/kafka-connect-iceberg-sink-0.1.4-SNAPSHOT-plugin.zip
  resources:
    requests:
      cpu: "0.1"
      memory: 512Mi
    limits:
      cpu: "3"
      memory: 2Gi
  template:
    connectContainer:
      env:
        # important for using AWS s3 client sdk
        - name: AWS_REGION
          value: "none"
```

KafkaConnector Debezium Source

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: postgres-source-connector
  labels:
    strimzi.io/cluster: my-connect-cluster
spec:
  class: io.debezium.connector.postgresql.PostgresConnector
  tasksMax: 1
  config:
    tasks.max: 1
    topic.prefix: ""
    database.hostname: <databasehost>
    database.port: 5432
    database.user: <dbUser>
    database.password: <dbPassword>
    database.dbname: <databaseName>
    database.server.name: <databaseName>
    transforms: unwrap
    transforms.unwrap.type: io.debezium.transforms.ExtractNewRecordState
    transforms.unwrap.add.fields: op,table,source.ts_ms,db
    transforms.unwrap.add.headers: db
    transforms.unwrap.delete.handling.mode: rewrite
    transforms.unwrap.drop.tombstones: true
    offset.flush.interval.ms: 0
    max.batch.size: 4096 # default: 2048
    max.queue.size: 16384 # default: 8192
```

KafkaConnector Iceberg Sink:

```yaml
apiVersion: kafka.strimzi.io/v1beta2
kind: KafkaConnector
metadata:
  name: iceberg-debezium-sink-connector
  labels:
    strimzi.io/cluster: my-connect-cluster
  annotations:
    strimzi.io/restart: "true"
spec:
  class: com.getindata.kafka.connect.iceberg.sink.IcebergSink
  tasksMax: 1
  config:
    topics: "<topic>"
    table.namespace: ""
    table.prefix: ""
    table.auto-create: true
    table.write-format: "parquet"
    iceberg.name: "mycatalog"
    # Nessie catalog
    iceberg.catalog-impl: "org.apache.iceberg.nessie.NessieCatalog"
    iceberg.uri: "http://nessie:19120/api/v1"
    iceberg.ref: "main"
    iceberg.authentication.type: "NONE"
    # Warehouse
    iceberg.warehouse: "s3://warehouse"
    # Minio S3
    iceberg.io-impl: "org.apache.iceberg.aws.s3.S3FileIO"
    iceberg.s3.endpoint: "http://minio:9000"
    iceberg.s3.path-style-access: true
    iceberg.s3.access-key-id: ""
    iceberg.s3.secret-access-key: ""
    # Batch size tuning
    # See: https://stackoverflow.com/questions/51753883/increase-the-number-of-messages-read-by-a-kafka-consumer-in-a-single-poll
    # And the key prefix in Note: https://stackoverflow.com/a/66551961/2688589
    consumer.override.max.poll.records: 2000 # default: 500
```

### AWS authentication

#### Hadoop s3a

AWS credentials can be passed:

1. As part of sink configuration under keys `iceberg.fs.s3a.access.key` and `iceberg.fs.s3a.secret.key`
2. Using enviornment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`
3. As ~/.aws/config file

#### Iceberg S3FileIO

https://iceberg.apache.org/docs/latest/aws/#s3-fileio

```
iceberg.warehouse: "s3://warehouse"
iceberg.io-impl: "org.apache.iceberg.aws.s3.S3FileIO"
iceberg.s3.endpoint: "http://minio:9000"
iceberg.s3.path-style-access: true
iceberg.s3.access-key-id: ''
iceberg.s3.secret-access-key: ''
```

### Catalogs

Using `GlueCatalog`

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "iceberg.catalog-impl": "org.apache.iceberg.aws.glue.GlueCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    "iceberg.fs.s3a.access.key": "my-aws-access-key",
    "iceberg.fs.s3a.secret.key": "my-secret-access-key",
    ...
  }
}
```

Using `HadoopCatalog`

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "iceberg.catalog-impl": "org.apache.iceberg.hadoop.HadoopCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    ...
  }
}
```

Using `HiveCatalog`

```json
{
  "name": "iceberg-sink",
  "config": {
    "connector.class": "com.getindata.kafka.connect.iceberg.sink.IcebergSink",
    "iceberg.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
    "iceberg.warehouse": "s3a://my_bucket/iceberg",
    "iceberg.uri": "thrift://localhost:9083",
    ...
  }
}
```

## Limitations

### DDL support

Creation of new tables and extending them with new columns is supported. Sink is not doing any operations that would affect multiple rows, because of that in case of table or column deletion no data is actually removed. This can be an issue when column is dropped and then recreated with a different type. This operation can crash the sink as it will try to write new data to a still exisitng column of a different data type.

Similar problem is with changing optionality of a column. If it was not defined as required when table was first created, sink will not check if such constrain can be introduced and will ignore that.

### DML

Rows cannot be updated nor removed unless primary key is defined. In case of deletion sink behavior is also dependent on upsert.keep-deletes option. When this option is set to true sink will leave a tombstone behind in a form of row containing only a primary key value and \_\_deleted flat set to true. When option is set to false it will remove row entirely.

### Iceberg partitioning support

The consumer reads unix millisecond timestamps from the event field configured in `iceberg.partition.timestamp`, converts them to iceberg
timestamps, and writes them to the table column configured in `iceberg.partition.column`. The timestamp column is then used to extract a
date to be used as the partitioning key. If `iceberg.partition.timestamp` is empty, `iceberg.parition.column` is assumed to already be of
type timestamp, and no conversion is performed. If they are set to the same value, the integer values will be replaced by the converted
timestamp values.

Partitioning only works when configured in append-only mode (`upsert: false`).

By default, the sink expects to receive events produced by a debezium source containing a source time at which the transaction was committed:

```sql
"sourceOffset": {
  ...
  "ts_ms": "1482918357011"
}
```

## Debezium change event format support

Kafka Connect Iceberg Sink is expecting events in a format of _Debezium change event_. It uses however only an _after_ portion of that event and some metadata.
Minimal fields needed for the sink to work are:

Kafka event key:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "int32",
        "optional": false,
        "field": "some_field"
      }
    ],
    "optional": false,
    "name": "some_event.Key"
  },
  "payload": {
    "id": 1
  }
}
```

Kafka event value:

```json
{
  "schema": {
    "type": "struct",
    "fields": [
      {
        "type": "struct",
        "fields": [
          {
            "type": "int64",
            "optional": false,
            "field": "field_name"
          },
          ...
        ],
        "optional": true,
        "name": "some_event.Value",
        "field": "after"
      },
      {
        "type": "struct",
        "fields": [
          {
            "type": "int64",
            "optional": false,
            "field": "ts_ms"
          },
          {
            "type": "string",
            "optional": false,
            "field": "db"
          },
          {
            "type": "string",
            "optional": false,
            "field": "table"
          }
        ],
        "optional": false,
        "name": "io.debezium.connector.postgresql.Source",
        "field": "source"
      },
      {
        "type": "string",
        "optional": false,
        "field": "op"
      }
    ],
    "optional": false,
    "name": "some_event.Envelope"
  },
  "payload": {
    "before": null,
    "after": {
      "some_field": 1,
      ...
    },
    "source": {
      "ts_ms": 1645448938851,
      "db": "some_source",
      "table": "some_table"
    },
    "op": "c"
  }
}
```
