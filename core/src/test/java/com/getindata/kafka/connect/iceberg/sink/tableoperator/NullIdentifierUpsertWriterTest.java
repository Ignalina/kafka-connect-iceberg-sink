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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A Debezium key column can arrive null in the value. Identifier columns are required in the Iceberg
 * schema, so such a row used to kill the task with an NPE in the Parquet column writer, first in the
 * equality delete (which carries only the identifier columns), and it would die the same way in the
 * data file. Now the row is skipped and counted, whatever its op, and the writer stays alive.
 */
class NullIdentifierUpsertWriterTest {
  private static final Schema SCHEMA = new Schema(
      List.of(
          Types.NestedField.required(1, "id", Types.IntegerType.get()),
          Types.NestedField.optional(2, "name", Types.StringType.get()),
          Types.NestedField.optional(3, "__op", Types.StringType.get())),
      Set.of(1));

  private BaseTaskWriter<Record> upsertWriter(Path dir) {
    Map<String, String> props = new HashMap<>();
    IcebergSinkConfiguration configuration = new IcebergSinkConfiguration(props);
    TableSettings settings = TableSettings.defaults(configuration);
    assertTrue(settings.isUpsert());

    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), dir.toString());
    catalog.createNamespace(Namespace.of("lake"));
    Table table = IcebergUtil.createIcebergTable(catalog, TableIdentifier.of("lake", "cdc_shop_orders"), SCHEMA, settings);
    assertEquals(Set.of(1), table.schema().identifierFieldIds());

    BaseTaskWriter<Record> writer = new IcebergTableWriterFactory(configuration).create(table, settings);
    assertTrue(writer instanceof UnpartitionedDeltaWriter);
    return writer;
  }

  private static GenericRecord record(Integer id, String op) {
    GenericRecord r = GenericRecord.create(SCHEMA);
    r.setField("id", id);
    r.setField("name", "order");
    r.setField("__op", op);
    return r;
  }

  @Test
  void updateWithNullIdentifierIsSkippedAndWriterSurvives(@TempDir Path dir) throws Exception {
    BaseTaskWriter<Record> writer = upsertWriter(dir);
    writer.write(record(null, CdcOperation.UPDATE.getCode()));
    // the writer is still usable after the bad row: a good one lands as usual
    writer.write(record(8, CdcOperation.UPDATE.getCode()));
    writer.close();
    WriteResult result = writer.complete();
    assertEquals(1, result.dataFiles().length);
    assertEquals(1, result.deleteFiles().length);
    assertEquals(1, ((BaseDeltaTaskWriter) writer).nullKeyCount("id"));
  }

  @Test
  void updateWithIdentifierWritesEqualityDeleteAndData(@TempDir Path dir) throws Exception {
    BaseTaskWriter<Record> writer = upsertWriter(dir);
    writer.write(record(7, CdcOperation.UPDATE.getCode()));
    writer.close();
    WriteResult result = writer.complete();
    assertEquals(1, result.dataFiles().length);
    assertEquals(1, result.deleteFiles().length);
    assertEquals(0, ((BaseDeltaTaskWriter) writer).nullKeyCount("id"));
  }

  @Test
  void deleteWithNullIdentifierIsSkipped(@TempDir Path dir) throws Exception {
    BaseTaskWriter<Record> writer = upsertWriter(dir);
    writer.write(record(null, CdcOperation.DELETE.getCode()));
    writer.close();
    WriteResult result = writer.complete();
    assertEquals(0, result.dataFiles().length);
    assertEquals(0, result.deleteFiles().length);
    assertEquals(1, ((BaseDeltaTaskWriter) writer).nullKeyCount("id"));
  }
}
