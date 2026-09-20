package com.getindata.kafka.connect.iceberg.sink;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * A cheap estimate of the payload size of a SinkRecord (key + value), used for the byte budget of the
 * task's flush buffer. Exact enough for a budget: bytes and strings by length, primitives at 8 bytes,
 * containers recursively.
 */
public final class SinkRecordSizer {
  private SinkRecordSizer() {
  }

  public static long estimate(SinkRecord record) {
    return 64 + sizeOf(record.key()) + sizeOf(record.value());
  }

  public static long sizeOf(Object value) {
    if (value == null) {
      return 0;
    }
    if (value instanceof byte[]) {
      return ((byte[]) value).length;
    }
    if (value instanceof ByteBuffer) {
      return ((ByteBuffer) value).remaining();
    }
    if (value instanceof CharSequence) {
      return ((CharSequence) value).length();
    }
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      long total = 16;
      for (Field field : struct.schema().fields()) {
        total += 8 + sizeOf(struct.get(field));
      }
      return total;
    }
    if (value instanceof Map) {
      long total = 16;
      for (Map.Entry<?, ?> e : ((Map<?, ?>) value).entrySet()) {
        total += sizeOf(e.getKey()) + sizeOf(e.getValue());
      }
      return total;
    }
    if (value instanceof List) {
      long total = 16;
      for (Object o : (List<?>) value) {
        total += sizeOf(o);
      }
      return total;
    }
    return 8;
  }
}
