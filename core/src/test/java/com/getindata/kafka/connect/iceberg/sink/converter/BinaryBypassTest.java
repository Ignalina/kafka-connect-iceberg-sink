package com.getindata.kafka.connect.iceberg.sink.converter;

import com.getindata.kafka.connect.iceberg.sink.IcebergChangeEvent;
import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BinaryBypassTest {
  private static final Schema INNER = SchemaBuilder.struct().name("inner")
      .field("doc", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("note", Schema.OPTIONAL_STRING_SCHEMA)
      .build();
  private static final Schema VALUE = SchemaBuilder.struct().name("row")
      .field("id", Schema.INT32_SCHEMA)
      .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("empty", Schema.OPTIONAL_BYTES_SCHEMA)
      .field("amount", Decimal.builder(2).optional().parameter("connect.decimal.precision", "10").build())
      .field("nested", INNER)
      .field("__source_ts_ms", Schema.OPTIONAL_INT64_SCHEMA)
      .build();
  private static final Schema KEY = SchemaBuilder.struct().name("key").field("id", Schema.INT32_SCHEMA).build();

  private static final Transformation<SinkRecord> IDENTITY = new Transformation<>() {
    @Override public SinkRecord apply(SinkRecord r) { return r; }
    @Override public ConfigDef config() { return new ConfigDef(); }
    @Override public void close() { }
    @Override public void configure(Map<String, ?> configs) { }
  };

  private static SinkRecordToIcebergChangeEventConverter converter(boolean bypass) {
    Map<String, String> props = new HashMap<>();
    props.put(IcebergSinkConfiguration.BINARY_BYPASS, String.valueOf(bypass));
    return new SinkRecordToIcebergChangeEventConverter(IDENTITY,
        JsonConverterFactory.create(true), JsonConverterFactory.create(false), new IcebergSinkConfiguration(props));
  }

  private static SinkRecord record() {
    byte[] blob = new byte[70_000];
    for (int i = 0; i < blob.length; i++) blob[i] = (byte) i;
    Struct inner = new Struct(INNER).put("doc", ByteBuffer.wrap(new byte[]{5, 6, 7})).put("note", "n");
    Struct v = new Struct(VALUE)
        .put("id", 1)
        .put("blob", blob)
        .put("empty", new byte[0])
        .put("amount", new BigDecimal("12.34"))
        .put("nested", inner)
        .put("__source_ts_ms", 1704067200000L);
    Struct k = new Struct(KEY).put("id", 1);
    return new SinkRecord("shop.dbo.orders", 0, KEY, k, VALUE, v, 7);
  }

  private static void assertRecord(IcebergChangeEvent e) {
    org.apache.iceberg.Schema s = e.icebergSchema("__source_ts");
    assertEquals(Types.BinaryType.get(), s.findField("blob").type());
    assertEquals(Types.DecimalType.of(10, 2), s.findField("amount").type());
    assertEquals(Types.BinaryType.get(), s.findField("nested.doc").type());
    GenericRecord r = e.asIcebergRecord(s, "__source_ts", "__source_ts_ms");
    ByteBuffer blob = (ByteBuffer) r.getField("blob");
    assertEquals(70_000, blob.remaining());
    assertEquals((byte) 69_999, blob.get(69_999));
    assertEquals(ByteBuffer.wrap(new byte[0]), r.getField("empty"));
    assertEquals(new BigDecimal("12.34"), r.getField("amount"));
    Record nested = (Record) r.getField("nested");
    assertEquals(ByteBuffer.wrap(new byte[]{5, 6, 7}), nested.getField("doc"));
    assertEquals("n", nested.getField("note"));
    assertEquals(1, r.getField("id"));
    assertEquals("shop_dbo_orders", e.destinationTable());
    assertTrue(s.identifierFieldNames().contains("id"));
  }

  @Test
  void bypassedBlobsSkipTheJsonRenderingButLandInTheRecord() {
    IcebergChangeEvent e = converter(true).convert(record());
    // the JSON rendering carries an empty placeholder, not 93 KB of base64
    assertEquals("", e.value().get("blob").asText());
    assertEquals("", e.value().get("nested").get("doc").asText());
    assertRecord(e);
  }

  @Test
  void withoutBypassTheResultIsTheSame() {
    IcebergChangeEvent e = converter(false).convert(record());
    assertTrue(e.value().get("blob").asText().length() > 90_000);
    assertRecord(e);
  }

  @Test
  void nullValueRecordsHaveNoPayload() {
    SinkRecord tombstone = new SinkRecord("shop.dbo.orders", 0, KEY, new Struct(KEY).put("id", 1), null, null, 8);
    IcebergChangeEvent e = converter(true).convert(tombstone);
    assertNull(e.value());
  }

  @Test
  void droppedTombstonesConvertToNull() {
    // ExtractNewRecordState with drop.tombstones=true returns null for Debezium's post-delete marker
    Transformation<SinkRecord> dropAll = new Transformation<>() {
      @Override public SinkRecord apply(SinkRecord r) { return null; }
      @Override public ConfigDef config() { return new ConfigDef(); }
      @Override public void close() { }
      @Override public void configure(Map<String, ?> configs) { }
    };
    SinkRecordToIcebergChangeEventConverter c = new SinkRecordToIcebergChangeEventConverter(dropAll,
        JsonConverterFactory.create(true), JsonConverterFactory.create(false), new IcebergSinkConfiguration(new HashMap<>()));
    assertNull(c.convert(record()));
  }

  @Test
  void detachLeavesTheOriginalStructUntouched() {
    SinkRecord r = record();
    Map<String, Object> detached = new HashMap<>();
    Struct copy = SinkRecordToIcebergChangeEventConverter.detachBinary((Struct) r.value(), "", detached);
    assertEquals(2, detached.size());
    assertTrue(detached.containsKey("blob") && detached.containsKey("nested.doc"));
    assertEquals(70_000, ((byte[]) ((Struct) r.value()).get("blob")).length);
    assertEquals(0, ((byte[]) copy.get("blob")).length);
    assertEquals(new BigDecimal("12.34"), copy.get("amount"));
  }
}
