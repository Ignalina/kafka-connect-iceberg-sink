package com.getindata.kafka.connect.iceberg.sink;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.common.config.ConfigDef.Importance.LOW;
import static org.apache.kafka.common.config.ConfigDef.Importance.MEDIUM;
import static org.apache.kafka.common.config.ConfigDef.Type.BOOLEAN;
import static org.apache.kafka.common.config.ConfigDef.Type.INT;
import static org.apache.kafka.common.config.ConfigDef.Type.LONG;
import static org.apache.kafka.common.config.ConfigDef.Type.STRING;

public class IcebergSinkConfiguration {
    public static final String UPSERT = "upsert";
    public static final String UPSERT_KEEP_DELETES = "upsert.keep-deletes";
    public static final String UPSERT_DEDUP_COLUMN = "upsert.dedup-column";
    public static final String UPSERT_OP_COLUMN = "upsert.op-column";
    public static final String ALLOW_FIELD_ADDITION = "allow-field-addition";
    public static final String TABLE_NAMESPACE = "table.namespace";
    public static final String TABLE_PREFIX = "table.prefix";
    public static final String TABLE_AUTO_CREATE = "table.auto-create";
    public static final String TABLE_SNAKE_CASE = "table.snake-case";
    public static final String RICH_TEMPORAL_TYPES = "rich-temporal-types";
    public static final String TYPES_UUID_AS_STRING = "types.uuid-as-string";
    public static final String TYPES_VARIABLE_SCALE_DECIMAL = "types.variable-scale-decimal";
    public static final String NAMESPACE_AUTO_CREATE = "table.namespace.auto-create";
    public static final String TABLE_RULES = TableSettings.RULES;
    public static final String NAMESPACE_NUKE = "table.namespace.nuke";
    public static final String FLUSH_SIZE = "flush.size";
    public static final String FLUSH_MAX_BYTES = "flush.max-bytes";
    public static final String BINARY_BYPASS = "binary.bypass-json";
    public static final String ICEBERG_PREFIX = "iceberg.";
    // with the trailing dot: upstream stripped only "iceberg.table-default" and produced properties like ".write.format.default"
    public static final String ICEBERG_TABLE_PREFIX = "iceberg.table-default.";
    public static final String CATALOG_NAME = ICEBERG_PREFIX + "name";
    public static final String CATALOG_IMPL = ICEBERG_PREFIX + "catalog-impl";
    public static final String CATALOG_TYPE = ICEBERG_PREFIX + "type";
    public static final String PARTITION_TIMESTAMP = ICEBERG_PREFIX + "partition.timestamp";
    public static final String PARTITION_COLUMN = ICEBERG_PREFIX + "partition.column";
    public static final String FORMAT_VERSION = ICEBERG_PREFIX + "format-version";


    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(UPSERT, BOOLEAN, true, MEDIUM,
                    "When true Iceberg rows will be updated based on table primary key. " +
                            "When false all modification will be added as separate rows.")
            .define(UPSERT_KEEP_DELETES, BOOLEAN, true, MEDIUM,
                    "When true delete operation will leave a tombstone that will have only " +
                            "a primary key and __deleted* flag set to true")
            .define(UPSERT_DEDUP_COLUMN, STRING, "__source_ts_ms", LOW,
                    "Column used to check which state is newer during upsert")
            .define(UPSERT_OP_COLUMN, STRING, "__op", LOW,
                    "Column used to check which state is newer during upsert when " +
                            "upsert.dedup-column is not enough to resolve")
            .define(ALLOW_FIELD_ADDITION, BOOLEAN, true, LOW,
                    "When true sink will be adding new columns to Iceberg tables on schema changes")
            .define(TABLE_AUTO_CREATE, BOOLEAN, false, MEDIUM,
                    "When true sink will automatically create new Iceberg tables")
            .define(TABLE_NAMESPACE, STRING, "default", MEDIUM,
                    "Table namespace. In Glue it will be used as database name")
            .define(TABLE_PREFIX, STRING, "", MEDIUM,
                    "Prefix added to all table names")
            .define(TABLE_SNAKE_CASE, BOOLEAN, false, MEDIUM,
                    "Coerce table names to snake_case")
            .define(RICH_TEMPORAL_TYPES, BOOLEAN, true, LOW,
                    "Deprecated and ignored: Debezium and Connect logical types (Date, Time, Timestamp, " +
                            "Decimal, Uuid, ...) are always mapped to their Iceberg types")
            .define(TYPES_UUID_AS_STRING, BOOLEAN, false, LOW,
                    "When true io.debezium.data.Uuid columns are created as string instead of Iceberg uuid " +
                            "(for readers that render uuid as binary)")
            .define(TYPES_VARIABLE_SCALE_DECIMAL, STRING, "string", LOW,
                    "How io.debezium.data.VariableScaleDecimal (numeric without scale) is stored: 'string' " +
                            "(lossless plain decimal string) or an integer scale N for decimal(38,N)")
            .define(NAMESPACE_AUTO_CREATE, BOOLEAN, true, MEDIUM,
                    "When true and table.auto-create is true, the table namespace is created in the catalog " +
                            "if it does not exist")
            .define(NAMESPACE_NUKE, STRING, "", LOW,
                    "DESTRUCTIVE. A token: at connector start drop every table with table.prefix in table.namespace " +
                            "from the catalog and delete their files in the object store. Kafka offsets are not " +
                            "touched. The token is stored on the namespace so the same value does not nuke twice")
            .define(FLUSH_SIZE, INT, 10000, MEDIUM,
                    "Records buffered across polls before they are written to Iceberg (one commit per table). " +
                            "The buffer is also written when flush.max-bytes is reached and on every offset commit")
            .define(FLUSH_MAX_BYTES, LONG, 256L * 1024 * 1024, MEDIUM,
                    "Byte budget of the flush buffer (estimated payload size); the buffer is written when " +
                            "reached, so one 200 MB row flushes alone while small rows fill up to flush.size")
            .define(BINARY_BYPASS, BOOLEAN, true, LOW,
                    "Hand plain bytes columns (BLOBs) straight from the Connect record to Iceberg instead of " +
                            "through the JSON (base64) rendering; saves ~3x the BLOB size in heap per row")
            .define(TABLE_RULES, STRING, "", MEDIUM,
                    "Comma separated ids of table.rule.<id>.* blocks in matching order (first match wins). " +
                            "A rule has a regex over the table name and overrides upsert, upsert.keep-deletes, " +
                            "identifier-columns, sort-order, partition, partition.column, partition.timestamp " +
                            "and iceberg.table-default.* for the tables it matches")
            .define(CATALOG_NAME, STRING, "default", MEDIUM,
                    "Iceberg catalog name")
            .define(CATALOG_IMPL, STRING, null, MEDIUM,
                    "Iceberg catalog implementation (Only one of iceberg.catalog-impl and iceberg.type " +
                            "can be set to non null value at the same time")
            .define(CATALOG_TYPE, STRING, null, MEDIUM,
                    "Iceberg catalog type (Only one of iceberg.catalog-impl and iceberg.type " +
                            "can be set to non null value at the same time)")
            .define(PARTITION_TIMESTAMP, STRING, "__source_ts_ms", MEDIUM,
                    "Unix millisecond timestamp used to fill the partitioning column. If set, values"+
                    "will be converted to a timestamp value and stored in iceberg.partition.column")
            .define(PARTITION_COLUMN, STRING, "__source_ts", MEDIUM,
                    "Column used for partitioning. If the column already exists, it must be of type timestamp.")
            .define(FORMAT_VERSION, STRING, "2", MEDIUM,
                    "Specification for the Iceberg table formatg. Version 1: Analytic Data Tables."+
                    "Version 2: Row-level Deletes. Default 2.");
                    
    private final AbstractConfig parsedConfig;
    private final Map<String, String> properties;
    private final List<TableSettings.Rule> tableRules;
    private final Map<String, TableSettings> settingsByTable = new ConcurrentHashMap<>();

    public IcebergSinkConfiguration(Map<String, String> properties) {
        this.properties = properties;
        parsedConfig = new AbstractConfig(CONFIG_DEF, properties);
        tableRules = TableSettings.parseRules(properties);
    }

    /** The parsed table.rule.<id>.* blocks, in matching order. */
    public List<TableSettings.Rule> getTableRules() {
        return tableRules;
    }

    /** The settings for one table (name without namespace, with prefix): defaults + first matching rule. */
    public TableSettings forTable(String tableName) {
        return settingsByTable.computeIfAbsent(tableName, name -> TableSettings.resolve(this, tableRules, name));
    }

    public boolean isUpsert() {
        return parsedConfig.getBoolean(UPSERT);
    }

    public boolean isUpsertKeepDelete() {
        return parsedConfig.getBoolean(UPSERT_KEEP_DELETES);
    }

    public String getUpsertDedupColumn() {
        return parsedConfig.getString(UPSERT_DEDUP_COLUMN);
    }

    public String getUpsertOpColumn() {
        return parsedConfig.getString(UPSERT_OP_COLUMN);
    }

    public boolean isAllowFieldAddition() {
        return parsedConfig.getBoolean(ALLOW_FIELD_ADDITION);
    }

    public boolean isTableAutoCreate() {
        return parsedConfig.getBoolean(TABLE_AUTO_CREATE);
    }

    public String getTableNamespace() {
        return parsedConfig.getString(TABLE_NAMESPACE);
    }

    public String getTablePrefix() {
        return parsedConfig.getString(TABLE_PREFIX);
    }

    public boolean isTableSnakeCase() {
        return parsedConfig.getBoolean(TABLE_SNAKE_CASE);
    }

    /** @deprecated logical types are always mapped; kept so old configurations still validate. */
    @Deprecated
    public boolean isRichTemporalTypes() {
        return true;
    }

    public boolean isUuidAsString() {
        return parsedConfig.getBoolean(TYPES_UUID_AS_STRING);
    }

    public String getVariableScaleDecimal() {
        return parsedConfig.getString(TYPES_VARIABLE_SCALE_DECIMAL);
    }

    public boolean isNamespaceAutoCreate() {
        return parsedConfig.getBoolean(NAMESPACE_AUTO_CREATE);
    }

    public String getNamespaceNuke() {
        return parsedConfig.getString(NAMESPACE_NUKE);
    }

    public int getFlushSize() {
        return parsedConfig.getInt(FLUSH_SIZE);
    }

    public long getFlushMaxBytes() {
        return parsedConfig.getLong(FLUSH_MAX_BYTES);
    }

    public boolean isBinaryBypass() {
        return parsedConfig.getBoolean(BINARY_BYPASS);
    }

    public String getCatalogName() {
        return parsedConfig.getString(CATALOG_NAME);
    }

    /**
     * Gets the name of the column used for partitioning the iceberg table.
     * @return Name of the partitioning column
     */
    public String getPartitionColumn() {
        return parsedConfig.getString(PARTITION_COLUMN);
    }

    public String getFormatVersion() {
        return parsedConfig.getString(FORMAT_VERSION);
    }

    /**
     * Gets the name of the column containing unix millisecond timestamps to be used for partitioning.
     * @return Unix timestamp in milliseconds
     */
    public String getPartitionTimestamp() {
        return parsedConfig.getString(PARTITION_TIMESTAMP);
    }

    public Map<String, String> getIcebergCatalogConfiguration() {
        return getConfiguration(ICEBERG_PREFIX);
    }

    public Map<String, String> getIcebergTableConfiguration() {
        return getConfiguration(ICEBERG_TABLE_PREFIX);
    }

    private Map<String, String> getConfiguration(String prefix) {
        Map<String, String> config = new HashMap<>();
        properties.keySet().stream().filter(key -> key.startsWith(prefix)).forEach(key -> {
            config.put(key.substring(prefix.length()), properties.get(key));
        });
        return config;
    }

    public static ConfigDef getConfigDef() {
        return CONFIG_DEF;
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
