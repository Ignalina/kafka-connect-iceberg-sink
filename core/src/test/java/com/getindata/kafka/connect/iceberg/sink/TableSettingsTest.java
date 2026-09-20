package com.getindata.kafka.connect.iceberg.sink;

import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.SortDirection;
import org.apache.iceberg.SortOrder;
import org.apache.iceberg.types.Types;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class TableSettingsTest {

  private static final Schema SCHEMA = new Schema(
      List.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "tenant", Types.StringType.get()),
          Types.NestedField.optional(3, "order_date", Types.DateType.get()),
          Types.NestedField.optional(4, "amount", Types.DecimalType.of(10, 2)),
          Types.NestedField.optional(5, "__source_ts", Types.TimestampType.withZone())),
      Set.of(1));

  private static Map<String, String> props(String... kv) {
    Map<String, String> m = new HashMap<>();
    for (int i = 0; i < kv.length; i += 2) {
      m.put(kv[i], kv[i + 1]);
    }
    return m;
  }

  @Test
  void defaultsWhenNoRuleMatches() {
    IcebergSinkConfiguration c = new IcebergSinkConfiguration(props(
        "upsert", "false",
        "iceberg.table-default.write.parquet.compression-codec", "zstd"));
    TableSettings s = c.forTable("cdc_shop_orders");
    assertEquals("", s.ruleId());
    assertFalse(s.isUpsert());
    assertEquals("__source_ts", s.getPartitionColumn());
    assertEquals("zstd", s.getTableProperties().get("write.parquet.compression-codec"));

    PartitionSpec spec = IcebergUtil.partitionSpec(SCHEMA, s);
    assertEquals(1, spec.fields().size());
    assertEquals("day", spec.fields().get(0).transform().toString());

    TableSettings upsert = c.forTable("x");
    assertEquals(spec.fields().get(0).sourceId(), 5);
    PartitionSpec none = IcebergUtil.partitionSpec(SCHEMA, TableSettings.defaults(
        new IcebergSinkConfiguration(props("upsert", "true"))));
    assertTrue(none.isUnpartitioned());
    assertFalse(upsert.isUpsert());
  }

  @Test
  void firstMatchingRuleWinsInDeclaredOrder() {
    IcebergSinkConfiguration c = new IcebergSinkConfiguration(props(
        "upsert", "true",
        "table.rules", "events,dims",
        "table.rule.dims.regex", "cdc_shop_",
        "table.rule.dims.upsert", "true",
        "table.rule.dims.partition", "bucket(id,16)",
        "table.rule.events.regex", "^cdc_shop_order",
        "table.rule.events.upsert", "false",
        "table.rule.events.upsert.keep-deletes", "false",
        "table.rule.events.partition", "day(order_date), tenant",
        "table.rule.events.sort-order", "tenant asc, order_date desc",
        "table.rule.events.iceberg.table-default.write.parquet.compression-codec", "zstd",
        "iceberg.table-default.commit.retry.num-retries", "9"));

    TableSettings events = c.forTable("cdc_shop_orders");
    assertEquals("events", events.ruleId());
    assertFalse(events.isUpsert());
    assertFalse(events.isUpsertKeepDeletes());
    assertEquals("zstd", events.getTableProperties().get("write.parquet.compression-codec"));
    assertEquals("9", events.getTableProperties().get("commit.retry.num-retries"));

    PartitionSpec spec = IcebergUtil.partitionSpec(SCHEMA, events);
    assertEquals(2, spec.fields().size());
    assertEquals("day", spec.fields().get(0).transform().toString());
    assertEquals(3, spec.fields().get(0).sourceId());
    assertEquals("identity", spec.fields().get(1).transform().toString());
    assertEquals(2, spec.fields().get(1).sourceId());

    SortOrder order = IcebergUtil.sortOrder(SCHEMA, events);
    assertEquals(2, order.fields().size());
    assertEquals(2, order.fields().get(0).sourceId());
    assertEquals(SortDirection.ASC, order.fields().get(0).direction());
    assertEquals(3, order.fields().get(1).sourceId());
    assertEquals(SortDirection.DESC, order.fields().get(1).direction());

    TableSettings dims = c.forTable("cdc_shop_customer");
    assertEquals("dims", dims.ruleId());
    assertTrue(dims.isUpsert());
    PartitionSpec dimSpec = IcebergUtil.partitionSpec(SCHEMA, dims);
    assertEquals("bucket[16]", dimSpec.fields().get(0).transform().toString());

    assertEquals("", c.forTable("cdc_other_x").ruleId());
  }

  @Test
  void identifierColumnsOverrideTheKey() {
    IcebergSinkConfiguration c = new IcebergSinkConfiguration(props(
        "table.rule.t.regex", ".*",
        "table.rule.t.identifier-columns", "tenant, id"));
    TableSettings s = c.forTable("anything");
    Schema withIds = IcebergUtil.withIdentifierColumns(SCHEMA, s);
    assertEquals(Set.of(1, 2), withIds.identifierFieldIds());
    assertTrue(withIds.findField("tenant").isRequired());
    // default sort order follows the identifier columns
    SortOrder order = IcebergUtil.sortOrder(withIds, s);
    assertEquals(2, order.fields().size());

    IcebergSinkConfiguration bad = new IcebergSinkConfiguration(props(
        "table.rule.t.regex", ".*",
        "table.rule.t.identifier-columns", "nope"));
    assertThrows(ConfigException.class, () -> IcebergUtil.withIdentifierColumns(SCHEMA, bad.forTable("x")));
  }

  @Test
  void badRulesFailAtConfigTime() {
    assertThrows(ConfigException.class, () -> new IcebergSinkConfiguration(props(
        "table.rule.t.upsert", "false")));                               // no regex
    assertThrows(ConfigException.class, () -> new IcebergSinkConfiguration(props(
        "table.rule.t.regex", "(", "table.rule.t.upsert", "false")));   // bad regex
    assertThrows(ConfigException.class, () -> new IcebergSinkConfiguration(props(
        "table.rule.t.regex", ".*", "table.rule.t.upsert", "maybe")));
    assertThrows(ConfigException.class, () -> new IcebergSinkConfiguration(props(
        "table.rule.t.regex", ".*", "table.rule.t.partitioning", "day(x)")));  // unknown key
    assertThrows(ConfigException.class, () -> new IcebergSinkConfiguration(props(
        "table.rules", "a,b", "table.rule.a.regex", ".*")));           // b undeclared

    IcebergSinkConfiguration c = new IcebergSinkConfiguration(props(
        "table.rule.t.regex", ".*", "table.rule.t.partition", "week(order_date)"));
    assertThrows(ConfigException.class, () -> IcebergUtil.partitionSpec(SCHEMA, c.forTable("x")));
    IcebergSinkConfiguration missing = new IcebergSinkConfiguration(props(
        "table.rule.t.regex", ".*", "table.rule.t.partition", "day(nope)"));
    assertThrows(ConfigException.class, () -> IcebergUtil.partitionSpec(SCHEMA, missing.forTable("x")));
  }
}
