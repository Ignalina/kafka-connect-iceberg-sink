package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;
import com.getindata.kafka.connect.iceberg.sink.IcebergUtil;
import com.getindata.kafka.connect.iceberg.sink.TableSettings;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.WriteResult;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** CDC batches arrive in commit order, not partition order: the append writer must fan out. */
class PartitionedAppendWriterTest {
  private static final Schema SCHEMA = new Schema(
      java.util.List.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "__source_ts", Types.TimestampType.withZone())),
      Set.of(1));

  @Test
  void interleavedPartitionsInOneBatchDoNotThrow(@TempDir Path dir) throws Exception {
    Map<String, String> props = new HashMap<>();
    props.put(IcebergSinkConfiguration.UPSERT, "false");
    props.put("table.rule.logs.regex", ".*");
    props.put("table.rule.logs.partition", "month(__source_ts)");
    IcebergSinkConfiguration configuration = new IcebergSinkConfiguration(props);
    TableSettings settings = configuration.forTable("cdc_shop_log");

    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), dir.toString());
    catalog.createNamespace(Namespace.of("lake"));
    Table table = IcebergUtil.createIcebergTable(catalog, TableIdentifier.of("lake", "cdc_shop_log"), SCHEMA, settings);
    assertEquals("month", table.spec().fields().get(0).transform().toString());

    BaseTaskWriter<Record> writer = new IcebergTableWriterFactory(configuration).create(table, settings);
    assertTrue(writer instanceof PartitionedAppendWriter);
    for (int i = 0; i < 20; i++) {
      GenericRecord r = GenericRecord.create(SCHEMA);
      r.setField("id", i);
      // July, August, September, July, August, September, ...
      r.setField("__source_ts", OffsetDateTime.of(2026, 7 + (i % 3), 1, 0, 0, 0, 0, ZoneOffset.UTC));
      writer.write(r);
    }
    writer.close();
    WriteResult result = writer.complete();
    assertEquals(3, result.dataFiles().length);
    assertEquals(0, result.deleteFiles().length);
  }
}
