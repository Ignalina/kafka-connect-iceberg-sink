package com.getindata.kafka.connect.iceberg.sink;

import org.apache.kafka.common.config.ConfigException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

/**
 * The settings that apply to ONE Iceberg table: the connector-wide defaults, overridden by the first
 * {@link Rule} whose regex matches the table name.
 *
 * <p>Rules are read from the connector properties:
 * <pre>
 *   table.rules=events,dims                      # order, first match wins (optional: else alphabetical)
 *   table.rule.events.regex=^cdc_shop_(order|invoice).*
 *   table.rule.events.upsert=false
 *   table.rule.events.partition=day(__source_ts)
 *   table.rule.events.sort-order=order_id asc,line_no asc
 *   table.rule.events.iceberg.table-default.write.parquet.compression-codec=zstd
 *   table.rule.dims.regex=^cdc_shop_dim_.*
 *   table.rule.dims.upsert=true
 *   table.rule.dims.identifier-columns=id
 *   table.rule.dims.partition=bucket(id,16)
 * </pre>
 * Column-level settings are the identifier columns (the row identity, the Debezium key by default),
 * the sort order (identifier columns ascending by default) and the partition transforms
 * ({@code identity(col)}, {@code year|month|day|hour(col)}, {@code bucket(col,N)},
 * {@code truncate(col,W)}); the default partitioning is {@code day(partition.column)} in append mode
 * and none in upsert mode.
 */
public final class TableSettings {
  public static final String RULES = "table.rules";
  public static final String RULE_PREFIX = "table.rule.";
  public static final String KEY_REGEX = "regex";
  public static final String KEY_UPSERT = "upsert";
  public static final String KEY_UPSERT_KEEP_DELETES = "upsert.keep-deletes";
  public static final String KEY_IDENTIFIER_COLUMNS = "identifier-columns";
  public static final String KEY_SORT_ORDER = "sort-order";
  public static final String KEY_PARTITION = "partition";
  public static final String KEY_PARTITION_COLUMN = "partition.column";
  public static final String KEY_PARTITION_TIMESTAMP = "partition.timestamp";
  public static final String KEY_TABLE_PROPERTIES_PREFIX = "iceberg.table-default.";

  private final String ruleId;
  private final boolean upsert;
  private final boolean upsertKeepDeletes;
  private final String partitionColumn;
  private final String partitionTimestamp;
  private final List<String> identifierColumns;
  private final List<String> sortOrder;
  private final List<String> partition;
  private final Map<String, String> tableProperties;

  private TableSettings(String ruleId, boolean upsert, boolean upsertKeepDeletes, String partitionColumn,
                        String partitionTimestamp, List<String> identifierColumns, List<String> sortOrder,
                        List<String> partition, Map<String, String> tableProperties) {
    this.ruleId = ruleId;
    this.upsert = upsert;
    this.upsertKeepDeletes = upsertKeepDeletes;
    this.partitionColumn = partitionColumn;
    this.partitionTimestamp = partitionTimestamp;
    this.identifierColumns = Collections.unmodifiableList(identifierColumns);
    this.sortOrder = Collections.unmodifiableList(sortOrder);
    this.partition = Collections.unmodifiableList(partition);
    this.tableProperties = Collections.unmodifiableMap(tableProperties);
  }

  /** The connector-wide defaults, no rule applied. */
  public static TableSettings defaults(IcebergSinkConfiguration configuration) {
    return new TableSettings("", configuration.isUpsert(), configuration.isUpsertKeepDelete(),
        configuration.getPartitionColumn(), configuration.getPartitionTimestamp(),
        Collections.emptyList(), Collections.emptyList(), Collections.emptyList(),
        configuration.getIcebergTableConfiguration());
  }

  /** The defaults with the first matching rule's overrides applied. */
  public static TableSettings resolve(IcebergSinkConfiguration configuration, List<Rule> rules, String tableName) {
    TableSettings base = defaults(configuration);
    for (Rule rule : rules) {
      if (rule.matches(tableName)) {
        return rule.applyTo(base);
      }
    }
    return base;
  }

  /** The rule id that produced these settings, "" for the connector defaults. */
  public String ruleId() {
    return ruleId;
  }

  public boolean isUpsert() {
    return upsert;
  }

  public boolean isUpsertKeepDeletes() {
    return upsertKeepDeletes;
  }

  public String getPartitionColumn() {
    return partitionColumn;
  }

  public String getPartitionTimestamp() {
    return partitionTimestamp;
  }

  /** Explicit identifier (row identity) columns; empty = the Debezium key columns. */
  public List<String> getIdentifierColumns() {
    return identifierColumns;
  }

  /** Sort order entries "column asc|desc"; empty = identifier columns ascending. */
  public List<String> getSortOrder() {
    return sortOrder;
  }

  /** Partition transforms; empty = day(partition column) in append mode, unpartitioned in upsert mode. */
  public List<String> getPartition() {
    return partition;
  }

  /** Iceberg table properties applied at create. */
  public Map<String, String> getTableProperties() {
    return tableProperties;
  }

  @Override
  public String toString() {
    return "TableSettings{rule='" + ruleId + "', upsert=" + upsert + ", keepDeletes=" + upsertKeepDeletes
        + ", partitionColumn='" + partitionColumn + "', identifierColumns=" + identifierColumns
        + ", sortOrder=" + sortOrder + ", partition=" + partition + ", tableProperties=" + tableProperties + '}';
  }

  /** Parse all {@code table.rule.<id>.*} properties, ordered by {@code table.rules} (else alphabetically). */
  public static List<Rule> parseRules(Map<String, String> properties) {
    Map<String, Map<String, String>> byId = new HashMap<>();
    for (Map.Entry<String, String> e : properties.entrySet()) {
      if (!e.getKey().startsWith(RULE_PREFIX)) {
        continue;
      }
      String rest = e.getKey().substring(RULE_PREFIX.length());
      int dot = rest.indexOf('.');
      if (dot <= 0) {
        throw new ConfigException(e.getKey(), e.getValue(), "expected " + RULE_PREFIX + "<id>.<setting>");
      }
      byId.computeIfAbsent(rest.substring(0, dot), k -> new HashMap<>()).put(rest.substring(dot + 1), e.getValue());
    }

    List<String> order = new ArrayList<>();
    String declared = properties.get(RULES);
    if (declared != null && !declared.trim().isEmpty()) {
      for (String id : declared.split(",")) {
        String trimmed = id.trim();
        if (!byId.containsKey(trimmed)) {
          throw new ConfigException(RULES, declared, "rule '" + trimmed + "' has no " + RULE_PREFIX + trimmed + "." + KEY_REGEX);
        }
        order.add(trimmed);
      }
      // rules not listed go last, alphabetically
      byId.keySet().stream().filter(id -> !order.contains(id)).sorted().forEach(order::add);
    } else {
      byId.keySet().stream().sorted().forEach(order::add);
    }

    List<Rule> rules = new ArrayList<>();
    for (String id : order) {
      rules.add(Rule.parse(id, byId.get(id)));
    }
    return rules;
  }

  /** Split on commas that are not inside parentheses, so {@code bucket(id,16), day(ts)} stays two entries. */
  static List<String> splitList(String value) {
    if (value == null || value.trim().isEmpty()) {
      return Collections.emptyList();
    }
    return java.util.Arrays.stream(value.split(",(?![^()]*\\))"))
        .map(String::trim)
        .filter(s -> !s.isEmpty())
        .collect(Collectors.toList());
  }

  /** One {@code table.rule.<id>} block: a regex over the table name plus optional overrides. */
  public static final class Rule {
    private final String id;
    private final Pattern regex;
    private final Boolean upsert;
    private final Boolean upsertKeepDeletes;
    private final String partitionColumn;
    private final String partitionTimestamp;
    private final List<String> identifierColumns;
    private final List<String> sortOrder;
    private final List<String> partition;
    private final Map<String, String> tableProperties;

    Rule(String id, Pattern regex, Boolean upsert, Boolean upsertKeepDeletes, String partitionColumn,
         String partitionTimestamp, List<String> identifierColumns, List<String> sortOrder,
         List<String> partition, Map<String, String> tableProperties) {
      this.id = id;
      this.regex = regex;
      this.upsert = upsert;
      this.upsertKeepDeletes = upsertKeepDeletes;
      this.partitionColumn = partitionColumn;
      this.partitionTimestamp = partitionTimestamp;
      this.identifierColumns = identifierColumns;
      this.sortOrder = sortOrder;
      this.partition = partition;
      this.tableProperties = tableProperties;
    }

    static Rule parse(String id, Map<String, String> settings) {
      String regexText = settings.get(KEY_REGEX);
      if (regexText == null || regexText.trim().isEmpty()) {
        throw new ConfigException(RULE_PREFIX + id + "." + KEY_REGEX, null, "every table rule needs a regex");
      }
      final Pattern regex;
      try {
        regex = Pattern.compile(regexText.trim());
      } catch (PatternSyntaxException e) {
        throw new ConfigException(RULE_PREFIX + id + "." + KEY_REGEX, regexText, e.getMessage());
      }
      Map<String, String> tableProperties = new HashMap<>();
      for (Map.Entry<String, String> e : settings.entrySet()) {
        String key = e.getKey();
        if (key.startsWith(KEY_TABLE_PROPERTIES_PREFIX)) {
          tableProperties.put(key.substring(KEY_TABLE_PROPERTIES_PREFIX.length()), e.getValue());
        } else if (!KNOWN_KEYS.contains(key)) {
          throw new ConfigException(RULE_PREFIX + id + "." + key, e.getValue(),
              "unknown table rule setting, expected one of " + KNOWN_KEYS + " or " + KEY_TABLE_PROPERTIES_PREFIX + "*");
        }
      }
      List<String> sortOrder = splitList(settings.get(KEY_SORT_ORDER));
      for (String entry : sortOrder) {
        String[] parts = entry.split("\\s+");
        if (parts.length > 2 || (parts.length == 2 && !parts[1].toLowerCase(Locale.ROOT).matches("asc|desc"))) {
          throw new ConfigException(RULE_PREFIX + id + "." + KEY_SORT_ORDER, settings.get(KEY_SORT_ORDER),
              "expected '<column> [asc|desc], ...'");
        }
      }
      return new Rule(id, regex,
          parseBoolean(id, KEY_UPSERT, settings.get(KEY_UPSERT)),
          parseBoolean(id, KEY_UPSERT_KEEP_DELETES, settings.get(KEY_UPSERT_KEEP_DELETES)),
          blankToNull(settings.get(KEY_PARTITION_COLUMN)),
          blankToNull(settings.get(KEY_PARTITION_TIMESTAMP)),
          splitList(settings.get(KEY_IDENTIFIER_COLUMNS)),
          sortOrder,
          splitList(settings.get(KEY_PARTITION)),
          tableProperties);
    }

    private static final List<String> KNOWN_KEYS = List.of(KEY_REGEX, KEY_UPSERT, KEY_UPSERT_KEEP_DELETES,
        KEY_IDENTIFIER_COLUMNS, KEY_SORT_ORDER, KEY_PARTITION, KEY_PARTITION_COLUMN, KEY_PARTITION_TIMESTAMP);

    private static Boolean parseBoolean(String id, String key, String value) {
      if (value == null || value.trim().isEmpty()) {
        return null;
      }
      String v = value.trim().toLowerCase(Locale.ROOT);
      if (v.equals("true") || v.equals("false")) {
        return Boolean.valueOf(v);
      }
      throw new ConfigException(RULE_PREFIX + id + "." + key, value, "expected true or false");
    }

    private static String blankToNull(String value) {
      return value == null || value.trim().isEmpty() ? null : value.trim();
    }

    public String id() {
      return id;
    }

    public boolean matches(String tableName) {
      return regex.matcher(tableName).find();
    }

    TableSettings applyTo(TableSettings base) {
      Map<String, String> props = new HashMap<>(base.tableProperties);
      props.putAll(tableProperties);
      return new TableSettings(id,
          upsert != null ? upsert : base.upsert,
          upsertKeepDeletes != null ? upsertKeepDeletes : base.upsertKeepDeletes,
          partitionColumn != null ? partitionColumn : base.partitionColumn,
          partitionTimestamp != null ? partitionTimestamp : base.partitionTimestamp,
          identifierColumns.isEmpty() ? base.identifierColumns : identifierColumns,
          sortOrder.isEmpty() ? base.sortOrder : sortOrder,
          partition.isEmpty() ? base.partition : partition,
          props);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof Rule)) return false;
      Rule rule = (Rule) o;
      return id.equals(rule.id) && regex.pattern().equals(rule.regex.pattern());
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, regex.pattern());
    }
  }
}
