package com.getindata.kafka.connect.iceberg.sink;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IcebergSinkTaskBufferTest {
  private static final Schema VALUE = SchemaBuilder.struct()
      .field("id", Schema.INT32_SCHEMA)
      .field("blob", Schema.OPTIONAL_BYTES_SCHEMA)
      .build();

  private static SinkRecord record(long offset, int blobBytes) {
    Struct v = new Struct(VALUE).put("id", (int) offset);
    if (blobBytes > 0) {
      v.put("blob", new byte[blobBytes]);
    }
    return new SinkRecord("t", 0, Schema.INT32_SCHEMA, (int) offset, VALUE, v, offset);
  }

  private static IcebergSinkTask task(List<List<SinkRecord>> batches, String size, String bytes) {
    Map<String, String> props = new HashMap<>();
    props.put(IcebergSinkConfiguration.FLUSH_SIZE, size);
    props.put(IcebergSinkConfiguration.FLUSH_MAX_BYTES, bytes);
    IcebergSinkTask task = new IcebergSinkTask();
    task.start((Collection<SinkRecord> c) -> batches.add(new ArrayList<>(c)), new IcebergSinkConfiguration(props));
    return task;
  }

  @Test
  void flushesOnRecordCount() {
    List<List<SinkRecord>> batches = new ArrayList<>();
    IcebergSinkTask task = task(batches, "3", "1000000");
    task.put(List.of(record(0, 0), record(1, 0)));
    assertEquals(0, batches.size());
    assertEquals(2, task.pendingRecords());
    task.put(List.of(record(2, 0), record(3, 0)));
    assertEquals(1, batches.size());
    assertEquals(3, batches.get(0).size());
    assertEquals(1, task.pendingRecords());
  }

  @Test
  void flushesOnByteBudgetSoABigRowGoesAlone() {
    List<List<SinkRecord>> batches = new ArrayList<>();
    IcebergSinkTask task = task(batches, "1000", "500");
    task.put(List.of(record(0, 10), record(1, 10)));
    assertEquals(0, batches.size());
    assertTrue(task.pendingBytes() > 0 && task.pendingBytes() < 500);
    task.put(List.of(record(2, 2000)));         // crosses the budget: flushed with the two small ones
    assertEquals(1, batches.size());
    assertEquals(3, batches.get(0).size());
    assertEquals(0, task.pendingRecords());
    task.put(List.of(record(3, 2000)));         // a lone big row flushes at once
    assertEquals(2, batches.size());
    assertEquals(1, batches.get(1).size());
  }

  @Test
  void offsetCommitAndCloseFlushWhatIsPending() {
    List<List<SinkRecord>> batches = new ArrayList<>();
    IcebergSinkTask task = task(batches, "1000", "1000000");
    task.put(List.of(record(0, 0)));
    Map<TopicPartition, OffsetAndMetadata> offsets = Map.of(new TopicPartition("t", 0), new OffsetAndMetadata(1));
    assertEquals(offsets, task.preCommit(offsets));
    assertEquals(1, batches.size());
    assertEquals(0, task.pendingRecords());
    task.preCommit(offsets);                    // nothing pending: no empty flush
    assertEquals(1, batches.size());
    task.put(List.of(record(1, 0)));
    task.close(List.of(new TopicPartition("t", 0)));
    assertEquals(2, batches.size());
  }
}
