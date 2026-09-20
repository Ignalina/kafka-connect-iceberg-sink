/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package com.getindata.kafka.connect.iceberg.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.relocated.com.google.common.primitives.Ints;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.iceberg.TableProperties.*;

/**
 * @author Ismail Simsek
 */
public class IcebergUtil {
  protected static final Logger LOGGER = LoggerFactory.getLogger(IcebergUtil.class);
  protected static final ObjectMapper jsonObjectMapper = new ObjectMapper();

  /** {@code transform(column[,arg])} or a bare column name (identity). */
  private static final Pattern TRANSFORM = Pattern.compile(
      "^\\s*(?:(identity|year|month|day|hour|bucket|truncate)\\s*\\(\\s*([^,()\\s]+)\\s*(?:,\\s*(\\d+)\\s*)?\\)|([^,()\\s]+))\\s*$",
      Pattern.CASE_INSENSITIVE);

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
      Schema schema, IcebergSinkConfiguration configuration) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, TableSettings.defaults(configuration),
        configuration.getFormatVersion());
  }

  public static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
      Schema schema, TableSettings settings) {
    return createIcebergTable(icebergCatalog, tableIdentifier, schema, settings, null);
  }

  private static Table createIcebergTable(Catalog icebergCatalog, TableIdentifier tableIdentifier,
      Schema schema, TableSettings settings, String formatVersionOverride) {
    schema = withIdentifierColumns(schema, settings);

    LOGGER.info("Creating table:'{}'\nschema:{}\nrowIdentifier:{}", tableIdentifier, schema,
        schema.identifierFieldNames());

    final PartitionSpec ps = partitionSpec(schema, settings);

    String formatVersion = "2";
    if (formatVersionOverride != null && !"".equals(formatVersionOverride)) {
      formatVersion = formatVersionOverride;
    } else if (settings.getTableProperties().get(FORMAT_VERSION) != null) {
      formatVersion = settings.getTableProperties().get(FORMAT_VERSION);
    }
    return icebergCatalog.buildTable(tableIdentifier, schema)
        .withProperties(settings.getTableProperties())
        .withProperty(FORMAT_VERSION, formatVersion)
        .withSortOrder(sortOrder(schema, settings))
        .withPartitionSpec(ps)
        .create();
  }

  /**
   * The schema with the rule's {@code identifier-columns} as row identity (marked required), or the
   * schema unchanged when the rule names none (the Debezium key columns stay the identity).
   */
  public static Schema withIdentifierColumns(Schema schema, TableSettings settings) {
    if (settings.getIdentifierColumns().isEmpty()) {
      return schema;
    }
    Set<Integer> ids = new HashSet<>();
    List<Types.NestedField> columns = new ArrayList<>();
    for (Types.NestedField column : schema.columns()) {
      if (settings.getIdentifierColumns().contains(column.name())) {
        ids.add(column.fieldId());
        columns.add(column.asRequired());
      } else {
        columns.add(column);
      }
    }
    for (String name : settings.getIdentifierColumns()) {
      if (schema.findField(name) == null) {
        throw new ConfigException("table.rule." + settings.ruleId() + "." + TableSettings.KEY_IDENTIFIER_COLUMNS,
            name, "column not found in table schema " + schema.columns().stream().map(Types.NestedField::name).toList());
      }
      if (!schema.findField(name).type().isPrimitiveType()) {
        throw new ConfigException("table.rule." + settings.ruleId() + "." + TableSettings.KEY_IDENTIFIER_COLUMNS,
            name, "identifier columns must be primitive");
      }
    }
    return new Schema(columns, ids);
  }

  /**
   * The partition spec for a new table: the rule's {@code partition} transforms when given, else
   * {@code day(partition column)} in append mode and unpartitioned in upsert mode.
   *
   * <p>Equality deletes are scoped to a partition, so an upsert table must be partitioned on values
   * that never change for a row (an identifier column, {@code bucket(id,N)}, a business date), never
   * on {@code __source_ts}.
   */
  public static PartitionSpec partitionSpec(Schema schema, TableSettings settings) {
    PartitionSpec.Builder builder = PartitionSpec.builderFor(schema);
    if (settings.getPartition().isEmpty()) {
      boolean partition = !settings.isUpsert();
      if (partition && settings.getPartitionColumn() != null && schema.findField(settings.getPartitionColumn()) != null) {
        builder.day(settings.getPartitionColumn());
      }
      return builder.build();
    }
    String key = "table.rule." + settings.ruleId() + "." + TableSettings.KEY_PARTITION;
    for (String entry : settings.getPartition()) {
      Matcher m = TRANSFORM.matcher(entry);
      if (!m.matches()) {
        throw new ConfigException(key, entry,
            "expected identity(col), year(col), month(col), day(col), hour(col), bucket(col,N), truncate(col,W) or a column name");
      }
      String transform = m.group(1) == null ? "identity" : m.group(1).toLowerCase(Locale.ROOT);
      String column = m.group(1) == null ? m.group(4) : m.group(2);
      String arg = m.group(3);
      if (schema.findField(column) == null) {
        throw new ConfigException(key, entry, "column '" + column + "' not found in table schema "
            + schema.columns().stream().map(Types.NestedField::name).toList());
      }
      if (settings.isUpsert() && column.equals(settings.getPartitionColumn())) {
        LOGGER.warn("Table rule '{}' partitions an upsert table on '{}': updates whose {} changes will not delete "
            + "the previous row (equality deletes are partition scoped)", settings.ruleId(), column, column);
      }
      switch (transform) {
        case "identity":
          builder.identity(column);
          break;
        case "year":
          builder.year(column);
          break;
        case "month":
          builder.month(column);
          break;
        case "day":
          builder.day(column);
          break;
        case "hour":
          builder.hour(column);
          break;
        case "bucket":
          if (arg == null) throw new ConfigException(key, entry, "bucket needs a bucket count: bucket(col,N)");
          builder.bucket(column, Integer.parseInt(arg));
          break;
        case "truncate":
          if (arg == null) throw new ConfigException(key, entry, "truncate needs a width: truncate(col,W)");
          builder.truncate(column, Integer.parseInt(arg));
          break;
        default:
          throw new ConfigException(key, entry, "unknown transform " + transform);
      }
    }
    return builder.build();
  }

  /** The rule's {@code sort-order} ("col asc, col2 desc") or the identifier columns ascending. */
  public static SortOrder sortOrder(Schema schema, TableSettings settings) {
    SortOrder.Builder sob = SortOrder.builderFor(schema);
    if (settings.getSortOrder().isEmpty()) {
      for (String fieldName : schema.identifierFieldNames()) {
        sob = sob.asc(fieldName);
      }
      return sob.build();
    }
    String key = "table.rule." + settings.ruleId() + "." + TableSettings.KEY_SORT_ORDER;
    for (String entry : settings.getSortOrder()) {
      String[] parts = entry.trim().split("\\s+");
      String column = parts[0];
      if (schema.findField(column) == null) {
        throw new ConfigException(key, entry, "column '" + column + "' not found in table schema "
            + schema.columns().stream().map(Types.NestedField::name).toList());
      }
      boolean desc = parts.length > 1 && parts[1].equalsIgnoreCase("desc");
      sob = desc ? sob.desc(column) : sob.asc(column);
    }
    return sob.build();
  }

  public static Optional<Table> loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId) {
    try {
      Table table = icebergCatalog.loadTable(tableId);
      return Optional.of(table);
    } catch (NoSuchTableException e) {
      LOGGER.info("Table not found: {}", tableId.toString());
      return Optional.empty();
    }
  }

  public static FileFormat getTableFileFormat(Table icebergTable) {
    String formatAsString = icebergTable.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
    return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
  }

  public static GenericAppenderFactory getTableAppender(Table icebergTable) {
    return new GenericAppenderFactory(
        icebergTable.schema(),
        icebergTable.spec(),
        Ints.toArray(icebergTable.schema().identifierFieldIds()),
        icebergTable.schema(),
        null);
  }

  public static String toSnakeCase(String inputString) {

    StringBuilder sb = new StringBuilder();
    boolean lastUpper = true;
    boolean lastSeparator = false;

    for (Character c : inputString.toCharArray()) {

      if (Character.isUpperCase(c)) {

        if (!lastUpper) {

          if (!lastSeparator) {
            sb.append("_");
          }

          lastUpper = true;
        }

        sb.append(Character.toLowerCase(c));
        lastSeparator = false;
      }

      else {

        if (c == '_') {
          lastSeparator = true;
        }

        else {
          lastSeparator = false;
        }

        sb.append(c);
        lastUpper = false;
      }
    }

    return sb.toString();
  }
}
