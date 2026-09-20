/*
 *
 *  * Copyright memiiso Authors.
 *  *
 *  * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package com.getindata.kafka.connect.iceberg.sink;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * One Debezium change event, as the Connect JSON converter renders it (schema + payload),
 * with the mapping from Connect / Debezium logical types to Iceberg types.
 *
 * <p>The Connect schema of every field is a JSON node with {@code type}, an optional logical
 * {@code name} (e.g. {@code org.apache.kafka.connect.data.Decimal},
 * {@code io.debezium.time.MicroTimestamp}) and optional {@code parameters}. The logical name is
 * stored as the Iceberg field {@code doc} so that value conversion knows the source unit
 * (milli / micro / nano seconds) when it reads the table's schema back.
 *
 * @author Ismail Simsek
 */
public class IcebergChangeEvent {
  private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeEvent.class);

  // Kafka Connect logical types (time.precision.mode=connect, decimal.handling.mode=precise)
  static final String CONNECT_DECIMAL = "org.apache.kafka.connect.data.Decimal";
  static final String CONNECT_DATE = "org.apache.kafka.connect.data.Date";
  static final String CONNECT_TIME = "org.apache.kafka.connect.data.Time";
  static final String CONNECT_TIMESTAMP = "org.apache.kafka.connect.data.Timestamp";
  // Debezium logical types (time.precision.mode=adaptive*, the default)
  static final String DBZ_DATE = "io.debezium.time.Date";
  static final String DBZ_TIME = "io.debezium.time.Time";
  static final String DBZ_MICRO_TIME = "io.debezium.time.MicroTime";
  static final String DBZ_NANO_TIME = "io.debezium.time.NanoTime";
  static final String DBZ_TIMESTAMP = "io.debezium.time.Timestamp";
  static final String DBZ_MICRO_TIMESTAMP = "io.debezium.time.MicroTimestamp";
  static final String DBZ_NANO_TIMESTAMP = "io.debezium.time.NanoTimestamp";
  static final String DBZ_ZONED_TIMESTAMP = "io.debezium.time.ZonedTimestamp";
  static final String DBZ_ZONED_TIME = "io.debezium.time.ZonedTime";
  static final String DBZ_UUID = "io.debezium.data.Uuid";
  static final String DBZ_BITS = "io.debezium.data.Bits";
  static final String DBZ_VARIABLE_SCALE_DECIMAL = "io.debezium.data.VariableScaleDecimal";

  static final String PARAM_SCALE = "scale";
  static final String PARAM_DECIMAL_PRECISION = "connect.decimal.precision";
  static final String PARAM_LENGTH = "length";
  static final int MAX_DECIMAL_PRECISION = 38;

  /** Internal marker type for the synthesized partition column. */
  private static final String TYPE_TIMESTAMPTZ = "timestamptz";

  private final String destination;
  private final JsonNode value;
  private final JsonNode key;
  private final JsonSchema jsonSchema;
  private final IcebergSinkConfiguration configuration;
  /** Plain bytes columns taken out of the record before the JSON rendering, by dotted field path. */
  private final Map<String, Object> detachedBinary;

  public IcebergChangeEvent(String destination,
                            JsonNode value,
                            JsonNode key,
                            JsonNode valueSchema,
                            JsonNode keySchema, IcebergSinkConfiguration configuration) {
    this(destination, value, key, valueSchema, keySchema, configuration, Collections.emptyMap());
  }

  public IcebergChangeEvent(String destination,
                            JsonNode value,
                            JsonNode key,
                            JsonNode valueSchema,
                            JsonNode keySchema, IcebergSinkConfiguration configuration,
                            Map<String, Object> detachedBinary) {
    this.destination = destination;
    this.value = value;
    this.key = key;
    this.configuration = configuration;
    this.jsonSchema = new JsonSchema(valueSchema, keySchema);
    this.detachedBinary = detachedBinary == null ? Collections.emptyMap() : detachedBinary;
  }

  public JsonNode key() {
    return key;
  }

  public JsonNode value() {
    return value;
  }

  public JsonSchema jsonSchema() {
    return jsonSchema;
  }

  public Schema icebergSchema(String partitionColumn) {
    return jsonSchema.icebergSchema(partitionColumn);
  }

  public String destinationTable() {
    return destination.replace(".", "_").replace("-", "_");
  }

  public GenericRecord asIcebergRecord(Schema schema, String partitionColumn, String partitionTimestampColumn) {
    final GenericRecord record = asIcebergRecord(schema.asStruct(), value, "");

    if (partitionTimestampColumn != null && !partitionTimestampColumn.equals("")) {
      // if partitionTimestampColumn is set, convert it to a timestamp and store it in partitionColumn.
      if (value != null && value.has(partitionTimestampColumn) && value.get(partitionTimestampColumn) != null) {
        final long partitionTimestamp = value.get(partitionTimestampColumn).longValue();
        final OffsetDateTime odt = OffsetDateTime.ofInstant(Instant.ofEpochMilli(partitionTimestamp), ZoneOffset.UTC);
        record.setField(partitionColumn, odt);
      } else {
        record.setField(partitionColumn, null);
      }
    }
    return record;
  }

  // ---------------------------------------------------------------------------------------------
  // Value conversion: JSON payload -> Iceberg generic values, driven by the (table) schema
  // ---------------------------------------------------------------------------------------------

  private GenericRecord asIcebergRecord(Types.StructType tableFields, JsonNode data, String path) {
    LOGGER.debug("Processing nested field:{}", tableFields);
    GenericRecord record = GenericRecord.create(tableFields);

    for (Types.NestedField field : tableFields.fields()) {
      String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
      if (!detachedBinary.isEmpty() && detachedBinary.containsKey(fieldPath)) {
        record.setField(field.name(), detachedValue(field.type(), detachedBinary.get(fieldPath)));
        continue;
      }
      // Set value to null if json event don't have the field
      if (data == null || !data.has(field.name()) || data.get(field.name()) == null) {
        record.setField(field.name(), null);
        continue;
      }
      // get the value of the field from json event, map it to iceberg value
      JsonNode node = data.get(field.name());
      if (field.type().isStructType()) {
        record.setField(field.name(), node.isNull() ? null : asIcebergRecord(field.type().asStructType(), node, fieldPath));
      } else {
        record.setField(field.name(), toIcebergValue(field.type(), semanticOf(field), node));
      }
    }

    return record;
  }

  /** A detached bytes value in the shape Iceberg's writer wants for {@code type}. */
  private static Object detachedValue(Type type, Object raw) {
    ByteBuffer buffer = raw instanceof ByteBuffer ? ((ByteBuffer) raw).duplicate() : ByteBuffer.wrap((byte[]) raw);
    switch (type.typeId()) {
      case BINARY:
        return buffer;
      case FIXED: {
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        return bytes;
      }
      case STRING:
        return java.nio.charset.StandardCharsets.UTF_8.decode(buffer).toString();
      default:
        throw new RuntimeException("Detached bytes value for non-binary Iceberg type " + type);
    }
  }

  private static String semanticOf(Types.NestedField field) {
    return field.doc() == null ? "" : field.doc();
  }

  /**
   * Convert one JSON value to the Java value Iceberg's generic writer expects for {@code type}.
   *
   * @param type     the Iceberg type of the column (from the table schema)
   * @param semantic the Connect / Debezium logical type name the column was created from ("" if none)
   * @param node     the JSON value
   */
  private Object toIcebergValue(Type type, String semantic, JsonNode node) {
    if (node == null || node.isNull()) {
      return null;
    }
    LOGGER.trace("Converting value type:{} semantic:{}", type, semantic);
    switch (type.typeId()) {
      case INTEGER:
        return node.asInt();
      case LONG:
        return node.asLong();
      case FLOAT:
        return node.floatValue();
      case DOUBLE:
        return node.asDouble();
      case BOOLEAN:
        return node.asBoolean();
      case DATE:
        return LocalDate.ofEpochDay(node.asLong());
      case TIME:
        return toLocalTime(semantic, node);
      case TIMESTAMP:
        return toTimestamp((Types.TimestampType) type, semantic, node);
      case STRING:
        if (DBZ_VARIABLE_SCALE_DECIMAL.equals(semantic) && node.isObject()) {
          return variableScaleDecimal(node).toPlainString();
        }
        return node.isValueNode() ? node.asText(null) : node.toString();
      case UUID:
        return UUID.fromString(node.asText());
      case DECIMAL:
        return toDecimal((Types.DecimalType) type, semantic, node);
      case FIXED:
        return binaryBytes(node);
      case BINARY:
        return ByteBuffer.wrap(binaryBytes(node));
      case LIST: {
        Types.ListType listType = (Types.ListType) type;
        List<Object> out = new ArrayList<>();
        Iterator<JsonNode> it = node.iterator();
        while (it.hasNext()) {
          out.add(toIcebergValue(listType.elementType(), semanticOf(listType.fields().get(0)), it.next()));
        }
        return out;
      }
      case MAP:
        return toMap((Types.MapType) type, node);
      case STRUCT:
        // create it as struct, nested type (inside lists / maps: no detached bytes there)
        // recursive call to get nested data/record
        return asIcebergRecord(type.asStructType(), node, "");
      default:
        // default to String type
        // if the node is not a value node (method isValueNode returns false), convert it to string.
        return node.isValueNode() ? node.asText(null) : node.toString();
    }
  }

  private static LocalTime toLocalTime(String semantic, JsonNode node) {
    if (node.isTextual()) {
      // Debezium converts ZonedTime values to UTC on capture, so no information is lost by converting them
      // to LocalTimes here. Iceberg doesn't support a ZonedTime equivalent anyway.
      return OffsetTime.parse(node.asText()).toLocalTime();
    }
    if (!node.isNumber()) {
      throw new RuntimeException("Unrecognized JSON node type for Iceberg type TIME: " + node.getNodeType());
    }
    final long raw = node.asLong();
    final long nanos;
    switch (semantic) {
      case DBZ_TIME:
      case CONNECT_TIME:
        nanos = raw * 1_000_000L;
        break;
      case DBZ_NANO_TIME:
        nanos = raw;
        break;
      case DBZ_MICRO_TIME:
      default:
        nanos = raw * 1_000L;
        break;
    }
    return LocalTime.ofNanoOfDay(nanos);
  }

  private static Object toTimestamp(Types.TimestampType type, String semantic, JsonNode node) {
    final Instant instant;
    if (node.isTextual()) {
      OffsetDateTime odt = OffsetDateTime.parse(node.asText());
      if (type.shouldAdjustToUTC()) {
        return odt;
      }
      instant = odt.toInstant();
    } else if (node.isNumber()) {
      final long raw = node.asLong();
      switch (semantic) {
        case DBZ_TIMESTAMP:
        case CONNECT_TIMESTAMP:
          instant = Instant.ofEpochMilli(raw);
          break;
        case DBZ_NANO_TIMESTAMP:
          instant = Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000_000L), Math.floorMod(raw, 1_000_000_000L));
          break;
        case DBZ_MICRO_TIMESTAMP:
        default:
          instant = Instant.ofEpochSecond(Math.floorDiv(raw, 1_000_000L), Math.floorMod(raw, 1_000_000L) * 1_000L);
          break;
      }
    } else {
      throw new RuntimeException("Unrecognized JSON node type for Iceberg type TIMESTAMP: " + node.getNodeType());
    }
    return type.shouldAdjustToUTC()
        ? OffsetDateTime.ofInstant(instant, ZoneOffset.UTC)
        : LocalDateTime.ofInstant(instant, ZoneOffset.UTC);
  }

  private static BigDecimal toDecimal(Types.DecimalType type, String semantic, JsonNode node) {
    final BigDecimal decimal;
    if (node.isObject()) {
      // io.debezium.data.VariableScaleDecimal {scale, value}
      decimal = variableScaleDecimal(node);
    } else if (node.isTextual()) {
      // Connect Decimal, JsonConverter decimal.format=BASE64: unscaled two's complement bytes
      decimal = new BigDecimal(new BigInteger(Base64.getDecoder().decode(node.asText())), type.scale());
    } else if (node.isNumber()) {
      // JsonConverter decimal.format=NUMERIC
      decimal = node.decimalValue();
    } else {
      throw new RuntimeException("Unrecognized JSON node type for Iceberg type DECIMAL: " + node.getNodeType());
    }
    return decimal.setScale(type.scale(), RoundingMode.HALF_UP);
  }

  private static BigDecimal variableScaleDecimal(JsonNode node) {
    int scale = node.has(PARAM_SCALE) ? node.get(PARAM_SCALE).asInt() : 0;
    byte[] unscaled = binaryBytes(node.get("value"));
    return new BigDecimal(new BigInteger(unscaled), scale);
  }

  private static byte[] binaryBytes(JsonNode node) {
    try {
      return node.binaryValue();
    } catch (IOException e) {
      LOGGER.error("Failed to convert binary value to iceberg value", e);
      throw new RuntimeException("Failed Processing Event!", e);
    }
  }

  private Object toMap(Types.MapType mapType, JsonNode node) {
    Map<Object, Object> out = new LinkedHashMap<>();
    String valueSemantic = semanticOf(mapType.fields().get(1));
    if (node.isObject()) {
      // JsonConverter renders maps with string keys as objects
      Iterator<Map.Entry<String, JsonNode>> it = node.fields();
      while (it.hasNext()) {
        Map.Entry<String, JsonNode> e = it.next();
        out.put(mapKey(mapType.keyType(), e.getKey()), toIcebergValue(mapType.valueType(), valueSemantic, e.getValue()));
      }
    } else if (node.isArray()) {
      // ... and maps with non-string keys as an array of [key, value] pairs
      for (JsonNode pair : node) {
        out.put(toIcebergValue(mapType.keyType(), "", pair.get(0)),
                toIcebergValue(mapType.valueType(), valueSemantic, pair.get(1)));
      }
    } else {
      throw new RuntimeException("Unrecognized JSON node type for Iceberg type MAP: " + node.getNodeType());
    }
    return out;
  }

  private static Object mapKey(Type keyType, String key) {
    switch (keyType.typeId()) {
      case INTEGER:
        return Integer.parseInt(key);
      case LONG:
        return Long.parseLong(key);
      case BOOLEAN:
        return Boolean.parseBoolean(key);
      case DOUBLE:
        return Double.parseDouble(key);
      case FLOAT:
        return Float.parseFloat(key);
      default:
        return key;
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Schema conversion: Connect JSON schema -> Iceberg types
  // ---------------------------------------------------------------------------------------------

  private Type.PrimitiveType primitiveType(String fieldType, String logicalName, JsonNode parameters, String path) {
    switch (fieldType) {
      case "int8":
      case "int16":
      case "int32": // int 4 bytes
        switch (logicalName) {
          case DBZ_DATE:
          case CONNECT_DATE:
            return Types.DateType.get();
          case DBZ_TIME:
          case CONNECT_TIME:
            return Types.TimeType.get();
          default:
            return Types.IntegerType.get();
        }
      case "int64": // long 8 bytes
        switch (logicalName) {
          case DBZ_TIMESTAMP:
          case DBZ_MICRO_TIMESTAMP:
          case DBZ_NANO_TIMESTAMP:
          case CONNECT_TIMESTAMP:
            return Types.TimestampType.withoutZone();
          case DBZ_MICRO_TIME:
          case DBZ_NANO_TIME:
            return Types.TimeType.get();
          default:
            return Types.LongType.get();
        }
      case "float8":
      case "float16":
      case "float32": // float is represented in 32 bits,
        return Types.FloatType.get();
      case "float64": // double is represented in 64 bits
      case "double":
        return Types.DoubleType.get();
      case "boolean":
        return Types.BooleanType.get();
      case "string":
        switch (logicalName) {
          case DBZ_ZONED_TIMESTAMP:
            return Types.TimestampType.withZone();
          case DBZ_ZONED_TIME:
            return Types.TimeType.get();
          case DBZ_UUID:
            return configuration.isUuidAsString() ? Types.StringType.get() : Types.UUIDType.get();
          default:
            return Types.StringType.get();
        }
      case "bytes":
        switch (logicalName) {
          case CONNECT_DECIMAL:
            return decimalType(parameters, path);
          case DBZ_BITS: {
            int bits = intParameter(parameters, PARAM_LENGTH, -1);
            return bits > 0 ? Types.FixedType.ofLength((bits + 7) / 8) : Types.BinaryType.get();
          }
          default:
            return Types.BinaryType.get();
        }
      case TYPE_TIMESTAMPTZ:
        return Types.TimestampType.withZone();
      default:
        // default to String type
        LOGGER.warn("Field {} has unsupported Connect type '{}', storing it as string", path, fieldType);
        return Types.StringType.get();
    }
  }

  private static Types.DecimalType decimalType(JsonNode parameters, String path) {
    int scale = intParameter(parameters, PARAM_SCALE, 0);
    int precision = intParameter(parameters, PARAM_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION);
    if (precision > MAX_DECIMAL_PRECISION) {
      LOGGER.warn("Field {} has decimal precision {}, Iceberg allows at most {}: clamping, values wider than that will fail to write",
                  path, precision, MAX_DECIMAL_PRECISION);
      precision = MAX_DECIMAL_PRECISION;
    }
    if (precision < 1) {
      precision = MAX_DECIMAL_PRECISION;
    }
    if (scale > precision) {
      LOGGER.warn("Field {} has decimal scale {} > precision {}: widening precision", path, scale, precision);
      precision = Math.min(scale, MAX_DECIMAL_PRECISION);
    }
    return Types.DecimalType.of(precision, scale);
  }

  private Type variableScaleDecimalType() {
    String setting = configuration.getVariableScaleDecimal();
    if (setting == null || setting.isEmpty() || "string".equalsIgnoreCase(setting)) {
      return Types.StringType.get();
    }
    int scale = Integer.parseInt(setting.trim());
    return Types.DecimalType.of(MAX_DECIMAL_PRECISION, scale);
  }

  private static int intParameter(JsonNode parameters, String name, int defaultValue) {
    if (parameters == null || !parameters.has(name)) {
      return defaultValue;
    }
    try {
      return Integer.parseInt(parameters.get(name).asText().trim());
    } catch (NumberFormatException e) {
      return defaultValue;
    }
  }

  private static String textOrEmpty(JsonNode node) {
    return node == null || node.isMissingNode() || node.isNull() ? "" : node.asText("");
  }

  public class JsonSchema {
    private final JsonNode valueSchema;
    private final JsonNode keySchema;

    JsonSchema(JsonNode valueSchema, JsonNode keySchema) {
      this.valueSchema = valueSchema;
      this.keySchema = keySchema;
    }

    public JsonNode valueSchema() {
      return valueSchema;
    }

    public JsonNode keySchema() {
      return keySchema;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;
      JsonSchema that = (JsonSchema) o;
      return Objects.equals(valueSchema, that.valueSchema) && Objects.equals(keySchema, that.keySchema);
    }

    @Override
    public int hashCode() {
      return Objects.hash(valueSchema, keySchema);
    }

    //getIcebergFieldsFromEventSchema
    private List<Types.NestedField> KeySchemaFields() {
      if (keySchema != null && keySchema.has("fields") && keySchema.get("fields").isArray()) {
        LOGGER.debug(keySchema.toString());
        return structFields(keySchema, new int[]{0}, "key", null);
      }
      LOGGER.trace("Key schema not found!");
      return new ArrayList<>();
    }

    private List<Types.NestedField> valueSchemaFields(String partitionColumn) {
      if (valueSchema != null && valueSchema.has("fields") && valueSchema.get("fields").isArray()) {
        LOGGER.debug(valueSchema.toString());
        int[] ids = new int[]{0};
        List<Types.NestedField> columns = structFields(valueSchema, ids, "value", partitionColumn);
        boolean partitionColumnPresent = partitionColumn != null && !partitionColumn.isEmpty()
            && columns.stream().anyMatch(c -> c.name().equals(partitionColumn));
        if (partitionColumn != null && !partitionColumn.isEmpty() && !partitionColumnPresent) {
          columns.add(Types.NestedField.optional(++ids[0], partitionColumn, Types.TimestampType.withZone(), ""));
        }
        return columns;
      }
      LOGGER.trace("Event schema not found!");
      return new ArrayList<>();
    }

    public Schema icebergSchema(String partitionColumn) {

      if (this.valueSchema == null) {
        throw new RuntimeException("Failed to get event schema, event schema is null");
      }

      final List<Types.NestedField> tableColumns = valueSchemaFields(partitionColumn);

      if (tableColumns.isEmpty()) {
        throw new RuntimeException("Failed to get event schema, event schema has no fields!");
      }

      final List<Types.NestedField> keyColumns = KeySchemaFields();
      Set<Integer> identifierFieldIds = new HashSet<>();

      for (Types.NestedField ic : keyColumns) {
        boolean found = false;

        ListIterator<Types.NestedField> colsIterator = tableColumns.listIterator();
        while (colsIterator.hasNext()) {
          Types.NestedField tc = colsIterator.next();
          if (Objects.equals(tc.name(), ic.name())) {
            identifierFieldIds.add(tc.fieldId());
            // set column as required its part of identifier filed
            colsIterator.set(tc.asRequired());
            found = true;
            break;
          }
        }

        if (!found) {
          throw new ValidationException("Table Row identifier field `" + ic.name() + "` not found in table columns");
        }

      }

      return new Schema(tableColumns, identifierFieldIds);
    }

    /**
     * The fields of a Connect {@code struct} schema as Iceberg fields. Field ids are allocated from
     * {@code ids[0]}; they only need to be unique within the schema, the catalog reassigns them on create.
     *
     * @param partitionColumn when non-null this is the top-level struct: a primitive field with this name
     *                        is created as {@code timestamptz} (it is the partition column)
     */
    private List<Types.NestedField> structFields(JsonNode structSchema, int[] ids, String path, String partitionColumn) {
      List<Types.NestedField> schemaColumns = new ArrayList<>();
      LOGGER.debug("Converting Schema of: {}::{}", path, textOrEmpty(structSchema.get("type")));
      for (JsonNode fieldSchema : structSchema.get("fields")) {
        int fieldId = ++ids[0];
        String fieldName = fieldSchema.get("field").textValue();
        String fieldPath = path + "." + fieldName;
        String logicalName = textOrEmpty(fieldSchema.get("name"));
        LOGGER.debug("Processing Field: [{}] {}::{} ({})", fieldId, fieldPath, textOrEmpty(fieldSchema.get("type")), logicalName);

        final Type type;
        if (partitionColumn != null && !partitionColumn.isEmpty() && fieldName.equals(partitionColumn)
            && isPrimitiveConnectType(fieldSchema)) {
          // if it is the partition column, swap its type to timestamp
          type = Types.TimestampType.withZone();
        } else {
          type = icebergType(fieldSchema, ids, fieldPath);
        }
        // the logical name rides along as the field doc, value conversion reads the source unit from it
        String doc = type.isPrimitiveType() || DBZ_VARIABLE_SCALE_DECIMAL.equals(logicalName) ? logicalName : "";
        schemaColumns.add(Types.NestedField.optional(fieldId, fieldName, type, doc));
      }
      return schemaColumns;
    }

    private boolean isPrimitiveConnectType(JsonNode fieldSchema) {
      String t = textOrEmpty(fieldSchema.get("type"));
      return !(t.equals("array") || t.equals("map") || t.equals("struct"));
    }

    /** Any Connect schema node (a field, an array's items, a map's keys / values) to an Iceberg type. */
    private Type icebergType(JsonNode schemaNode, int[] ids, String path) {
      if (schemaNode == null || !schemaNode.has("type")) {
        throw new RuntimeException("Unexpected schema without type for field " + path);
      }
      String fieldType = schemaNode.get("type").textValue();
      String logicalName = textOrEmpty(schemaNode.get("name"));
      switch (fieldType) {
        case "array": {
          int elementId = ++ids[0];
          Type elementType = icebergType(schemaNode.get("items"), ids, path + ".element");
          return Types.ListType.ofOptional(elementId, elementType);
        }
        case "map": {
          int keyId = ++ids[0];
          int valueId = ++ids[0];
          Type keyType = icebergType(schemaNode.get("keys"), ids, path + ".key");
          Type valueType = icebergType(schemaNode.get("values"), ids, path + ".value");
          if (!keyType.isPrimitiveType()) {
            throw new RuntimeException("'" + path + "' has a Map with non-primitive keys, not supported!");
          }
          return Types.MapType.ofOptional(keyId, valueId, keyType, valueType);
        }
        case "struct":
          if (DBZ_VARIABLE_SCALE_DECIMAL.equals(logicalName)) {
            return variableScaleDecimalType();
          }
          return Types.StructType.of(structFields(schemaNode, ids, path, null));
        default: //primitive types
          return primitiveType(fieldType, logicalName, schemaNode.get("parameters"), path);
      }
    }

  }

}
