package com.getindata.kafka.connect.iceberg.sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Connect JSON schema + payload the sink sees after the JsonConverter (schemas.enable=true),
 * for every Debezium / Connect logical type, mapped to Iceberg types and values.
 */
class IcebergChangeEventTypesTest {
  private static final ObjectMapper M = new ObjectMapper();
  private static final String PARTITION_COLUMN = "__source_ts";

  private static ObjectNode field(String name, String type) {
    ObjectNode f = M.createObjectNode();
    f.put("type", type);
    f.put("optional", true);
    f.put("field", name);
    return f;
  }

  private static ObjectNode logical(String name, String type, String logicalName) {
    return field(name, type).put("name", logicalName);
  }

  private static ObjectNode withParams(ObjectNode f, String... kv) {
    ObjectNode p = f.putObject("parameters");
    for (int i = 0; i < kv.length; i += 2) {
      p.put(kv[i], kv[i + 1]);
    }
    return f;
  }

  private static ObjectNode struct(ObjectNode... fields) {
    ObjectNode s = M.createObjectNode();
    s.put("type", "struct");
    s.put("optional", false);
    ArrayNode arr = s.putArray("fields");
    for (ObjectNode f : fields) {
      arr.add(f);
    }
    return s;
  }

  private static String base64Unscaled(String decimal) {
    return Base64.getEncoder().encodeToString(new BigDecimal(decimal).unscaledValue().toByteArray());
  }

  private static IcebergChangeEvent event(ObjectNode valueSchema, ObjectNode value, Map<String, String> props) {
    ObjectNode keySchema = struct(field("id", "int32"));
    ObjectNode key = M.createObjectNode().put("id", value.get("id").asInt());
    return new IcebergChangeEvent("shop.dbo.orders", value, key, valueSchema, keySchema, new IcebergSinkConfiguration(props));
  }

  private static Type typeOf(Schema schema, String column) {
    return schema.findField(column).type();
  }

  @Test
  void logicalTypesBecomeIcebergTypes() {
    ObjectNode schema = struct(
        field("id", "int32"),
        withParams(logical("amount", "bytes", IcebergChangeEvent.CONNECT_DECIMAL), "scale", "2", "connect.decimal.precision", "10"),
        withParams(logical("wide", "bytes", IcebergChangeEvent.CONNECT_DECIMAL), "scale", "4", "connect.decimal.precision", "99"),
        logical("d", "int32", IcebergChangeEvent.DBZ_DATE),
        logical("cd", "int32", IcebergChangeEvent.CONNECT_DATE),
        logical("t_ms", "int32", IcebergChangeEvent.DBZ_TIME),
        logical("t_us", "int64", IcebergChangeEvent.DBZ_MICRO_TIME),
        logical("t_ns", "int64", IcebergChangeEvent.DBZ_NANO_TIME),
        logical("ts_ms", "int64", IcebergChangeEvent.DBZ_TIMESTAMP),
        logical("ts_us", "int64", IcebergChangeEvent.DBZ_MICRO_TIMESTAMP),
        logical("ts_ns", "int64", IcebergChangeEvent.DBZ_NANO_TIMESTAMP),
        logical("cts", "int64", IcebergChangeEvent.CONNECT_TIMESTAMP),
        logical("zts", "string", IcebergChangeEvent.DBZ_ZONED_TIMESTAMP),
        logical("zt", "string", IcebergChangeEvent.DBZ_ZONED_TIME),
        logical("u", "string", IcebergChangeEvent.DBZ_UUID),
        withParams(logical("bits", "bytes", IcebergChangeEvent.DBZ_BITS), "length", "12"),
        field("blob", "bytes"),
        field("plain_long", "int64"),
        field("__source_ts_ms", "int64")
    );
    ObjectNode value = M.createObjectNode();
    value.put("id", 7);
    value.put("amount", base64Unscaled("123.45"));
    value.put("wide", base64Unscaled("0.0001"));
    value.put("d", 19723);          // 2024-01-01
    value.put("cd", 19723);
    value.put("t_ms", 3_600_000);   // 01:00
    value.put("t_us", 3_600_000_000L);
    value.put("t_ns", 3_600_000_000_000L);
    value.put("ts_ms", 1704067200123L);
    value.put("ts_us", 1704067200123456L);
    value.put("ts_ns", 1704067200123456789L);
    value.put("cts", 1704067200123L);
    value.put("zts", "2024-01-01T00:00:00.123456+01:00");
    value.put("zt", "01:02:03.000004Z");
    value.put("u", "123e4567-e89b-12d3-a456-426614174000");
    value.put("bits", Base64.getEncoder().encodeToString(new byte[]{1, 2}));
    value.put("blob", Base64.getEncoder().encodeToString(new byte[]{9, 8, 7}));
    value.put("plain_long", 42L);
    value.put("__source_ts_ms", 1704067200000L);

    IcebergChangeEvent e = event(schema, value, new HashMap<>());
    Schema s = e.icebergSchema(PARTITION_COLUMN);

    assertEquals(Types.DecimalType.of(10, 2), typeOf(s, "amount"));
    assertEquals(Types.DecimalType.of(38, 4), typeOf(s, "wide"));
    assertEquals(Types.DateType.get(), typeOf(s, "d"));
    assertEquals(Types.DateType.get(), typeOf(s, "cd"));
    assertEquals(Types.TimeType.get(), typeOf(s, "t_ms"));
    assertEquals(Types.TimeType.get(), typeOf(s, "t_us"));
    assertEquals(Types.TimeType.get(), typeOf(s, "t_ns"));
    assertEquals(Types.TimestampType.withoutZone(), typeOf(s, "ts_ms"));
    assertEquals(Types.TimestampType.withoutZone(), typeOf(s, "ts_us"));
    assertEquals(Types.TimestampType.withoutZone(), typeOf(s, "ts_ns"));
    assertEquals(Types.TimestampType.withoutZone(), typeOf(s, "cts"));
    assertEquals(Types.TimestampType.withZone(), typeOf(s, "zts"));
    assertEquals(Types.TimeType.get(), typeOf(s, "zt"));
    assertEquals(Types.UUIDType.get(), typeOf(s, "u"));
    assertEquals(Types.FixedType.ofLength(2), typeOf(s, "bits"));
    assertEquals(Types.BinaryType.get(), typeOf(s, "blob"));
    assertEquals(Types.LongType.get(), typeOf(s, "plain_long"));
    assertEquals(Types.TimestampType.withZone(), typeOf(s, PARTITION_COLUMN));
    assertTrue(s.identifierFieldNames().contains("id"));
    assertEquals(IcebergChangeEvent.DBZ_MICRO_TIMESTAMP, s.findField("ts_us").doc());

    GenericRecord r = e.asIcebergRecord(s, PARTITION_COLUMN, "__source_ts_ms");
    assertEquals(new BigDecimal("123.45"), r.getField("amount"));
    assertEquals(new BigDecimal("0.0001"), r.getField("wide"));
    assertEquals(LocalDate.of(2024, 1, 1), r.getField("d"));
    assertEquals(LocalDate.of(2024, 1, 1), r.getField("cd"));
    assertEquals(LocalTime.of(1, 0), r.getField("t_ms"));
    assertEquals(LocalTime.of(1, 0), r.getField("t_us"));
    assertEquals(LocalTime.of(1, 0), r.getField("t_ns"));
    assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_000_000), r.getField("ts_ms"));
    assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_456_000), r.getField("ts_us"));
    assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_456_789), r.getField("ts_ns"));
    assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_000_000), r.getField("cts"));
    assertEquals(OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 123_456_000, ZoneOffset.ofHours(1)), r.getField("zts"));
    assertEquals(LocalTime.of(1, 2, 3, 4_000), r.getField("zt"));
    assertEquals(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"), r.getField("u"));
    assertEquals(ByteBuffer.wrap(new byte[]{1, 2}), ByteBuffer.wrap((byte[]) r.getField("bits")));
    assertEquals(ByteBuffer.wrap(new byte[]{9, 8, 7}), r.getField("blob"));
    assertEquals(42L, r.getField("plain_long"));
    assertEquals(OffsetDateTime.of(2024, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC), r.getField(PARTITION_COLUMN));
  }

  @Test
  void uuidCanStayString() {
    ObjectNode schema = struct(field("id", "int32"), logical("u", "string", IcebergChangeEvent.DBZ_UUID));
    ObjectNode value = M.createObjectNode().put("id", 1).put("u", "123e4567-e89b-12d3-a456-426614174000");
    Map<String, String> props = new HashMap<>();
    props.put(IcebergSinkConfiguration.TYPES_UUID_AS_STRING, "true");
    IcebergChangeEvent e = event(schema, value, props);
    Schema s = e.icebergSchema(PARTITION_COLUMN);
    assertEquals(Types.StringType.get(), typeOf(s, "u"));
    assertEquals("123e4567-e89b-12d3-a456-426614174000", e.asIcebergRecord(s, PARTITION_COLUMN, "").getField("u"));
  }

  @Test
  void variableScaleDecimalStringOrScaled() {
    ObjectNode vsd = struct(field("scale", "int32"), field("value", "bytes"));
    vsd.put("name", IcebergChangeEvent.DBZ_VARIABLE_SCALE_DECIMAL);
    vsd.put("field", "n");
    ObjectNode schema = struct(field("id", "int32"), vsd);
    ObjectNode value = M.createObjectNode().put("id", 1);
    value.putObject("n").put("scale", 3).put("value", Base64.getEncoder().encodeToString(BigInteger.valueOf(12345).toByteArray()));

    IcebergChangeEvent asString = event(schema, value, new HashMap<>());
    Schema s1 = asString.icebergSchema(PARTITION_COLUMN);
    assertEquals(Types.StringType.get(), typeOf(s1, "n"));
    assertEquals("12.345", asString.asIcebergRecord(s1, PARTITION_COLUMN, "").getField("n"));

    Map<String, String> props = new HashMap<>();
    props.put(IcebergSinkConfiguration.TYPES_VARIABLE_SCALE_DECIMAL, "6");
    IcebergChangeEvent asDecimal = event(schema, value, props);
    Schema s2 = asDecimal.icebergSchema(PARTITION_COLUMN);
    assertEquals(Types.DecimalType.of(38, 6), typeOf(s2, "n"));
    assertEquals(new BigDecimal("12.345000"), asDecimal.asIcebergRecord(s2, PARTITION_COLUMN, "").getField("n"));
  }

  @Test
  void mapsAndNestedListsAreSupported() {
    ObjectNode mapField = field("attrs", "map");
    mapField.putObject("keys").put("type", "string").put("optional", false);
    mapField.putObject("values").put("type", "int64").put("optional", true);

    ObjectNode intKeyMap = field("counts", "map");
    intKeyMap.putObject("keys").put("type", "int32").put("optional", false);
    intKeyMap.putObject("values").put("type", "string").put("optional", true);

    ObjectNode nested = field("matrix", "array");
    ObjectNode inner = nested.putObject("items");
    inner.put("type", "array").put("optional", true);
    inner.putObject("items").put("type", "int32").put("optional", true);

    ObjectNode decimals = field("prices", "array");
    ObjectNode items = decimals.putObject("items");
    items.put("type", "bytes").put("optional", true).put("name", IcebergChangeEvent.CONNECT_DECIMAL);
    items.putObject("parameters").put("scale", "2").put("connect.decimal.precision", "8");

    ObjectNode structs = field("lines", "array");
    ObjectNode lineStruct = structs.putObject("items");
    lineStruct.put("type", "struct").put("optional", true);
    lineStruct.putArray("fields").add(field("qty", "int32")).add(logical("when", "int64", IcebergChangeEvent.DBZ_TIMESTAMP));

    ObjectNode schema = struct(field("id", "int32"), mapField, intKeyMap, nested, decimals, structs);

    ObjectNode value = M.createObjectNode().put("id", 1);
    value.putObject("attrs").put("a", 1L).put("b", 2L);
    ArrayNode pairs = value.putArray("counts");
    pairs.addArray().add(5).add("five");
    ArrayNode matrix = value.putArray("matrix");
    matrix.addArray().add(1).add(2);
    matrix.addArray().add(3);
    value.putArray("prices").add(base64Unscaled("1.50")).add(base64Unscaled("2.25"));
    ArrayNode lines = value.putArray("lines");
    lines.addObject().put("qty", 3).put("when", 1704067200123L);

    IcebergChangeEvent e = event(schema, value, new HashMap<>());
    Schema s = e.icebergSchema(PARTITION_COLUMN);
    assertEquals(Types.MapType.ofOptional(0, 0, Types.StringType.get(), Types.LongType.get()).valueType(), ((Types.MapType) typeOf(s, "attrs")).valueType());
    assertEquals(Types.IntegerType.get(), ((Types.MapType) typeOf(s, "counts")).keyType());
    assertTrue(((Types.ListType) typeOf(s, "matrix")).elementType().isListType());
    assertEquals(Types.DecimalType.of(8, 2), ((Types.ListType) typeOf(s, "prices")).elementType());
    Types.StructType line = ((Types.ListType) typeOf(s, "lines")).elementType().asStructType();
    assertEquals(Types.TimestampType.withoutZone(), line.field("when").type());

    GenericRecord r = e.asIcebergRecord(s, PARTITION_COLUMN, "");
    Map<?, ?> attrs = (Map<?, ?>) r.getField("attrs");
    assertEquals(1L, attrs.get("a"));
    assertEquals(2L, attrs.get("b"));
    Map<?, ?> counts = (Map<?, ?>) r.getField("counts");
    assertEquals("five", counts.get(5));
    List<?> m = (List<?>) r.getField("matrix");
    assertEquals(List.of(1, 2), m.get(0));
    assertEquals(List.of(3), m.get(1));
    assertEquals(List.of(new BigDecimal("1.50"), new BigDecimal("2.25")), r.getField("prices"));
    Record first = (Record) ((List<?>) r.getField("lines")).get(0);
    assertEquals(3, first.getField("qty"));
    assertEquals(LocalDateTime.of(2024, 1, 1, 0, 0, 0, 123_000_000), first.getField("when"));
  }

  @Test
  void missingAndNullValuesStayNull() {
    ObjectNode schema = struct(field("id", "int32"),
        withParams(logical("amount", "bytes", IcebergChangeEvent.CONNECT_DECIMAL), "scale", "2", "connect.decimal.precision", "10"),
        logical("ts", "int64", IcebergChangeEvent.DBZ_MICRO_TIMESTAMP));
    ObjectNode value = M.createObjectNode().put("id", 1);
    value.putNull("amount");
    IcebergChangeEvent e = event(schema, value, new HashMap<>());
    Schema s = e.icebergSchema(PARTITION_COLUMN);
    GenericRecord r = e.asIcebergRecord(s, PARTITION_COLUMN, "");
    assertNull(r.getField("amount"));
    assertNull(r.getField("ts"));
  }
}
