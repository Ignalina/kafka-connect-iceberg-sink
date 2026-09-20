package com.getindata.kafka.connect.iceberg.sink.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.getindata.kafka.connect.iceberg.sink.IcebergChangeEvent;
import com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * SinkRecord (Connect Struct, as the Avro converter delivers it) to {@link IcebergChangeEvent}.
 *
 * <p>The event is built from the Connect JSON rendering of the record (schema + payload), because
 * the type mapping reads the Connect schema as JSON. Plain {@code bytes} columns (BLOBs, no logical
 * type) are <b>detached</b> from the Struct before that rendering and handed to the event as-is:
 * they never become base64 text, are never parsed by Jackson and are never decoded again. A 200 MB
 * BLOB costs 200 MB, not four times that.
 */
public class SinkRecordToIcebergChangeEventConverter {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final byte[] EMPTY = new byte[0];

    private final Transformation<SinkRecord> extractNewRecordStateTransformation;
    private final JsonConverter keyJsonConverter;
    private final JsonConverter valueJsonConverter;
    private final IcebergSinkConfiguration configuration;

    public SinkRecordToIcebergChangeEventConverter(Transformation<SinkRecord> extractNewRecordStateTransformation,
                                                   JsonConverter keyJsonConverter,
                                                   JsonConverter valueJsonConverter,
                                                   IcebergSinkConfiguration configuration) {
        this.extractNewRecordStateTransformation = extractNewRecordStateTransformation;
        this.keyJsonConverter = keyJsonConverter;
        this.valueJsonConverter = valueJsonConverter;
        this.configuration = configuration;
    }

    public IcebergChangeEvent convert(SinkRecord record) {
        SinkRecord unwrappedRecord = extractNewRecordStateTransformation.apply(record);
        if (unwrappedRecord == null) {
            // Tombstone: the transformation is configured with
            // drop.tombstones=true and returns null. Nothing to write.
            return null;
        }

        Map<String, Object> detachedBinary = new HashMap<>();
        Object value = unwrappedRecord.value();
        if (configuration.isBinaryBypass() && value instanceof Struct) {
            value = detachBinary((Struct) value, "", detachedBinary);
        }

        byte[] keyBytes = keyJsonConverter.fromConnectData(unwrappedRecord.topic(), unwrappedRecord.keySchema(), unwrappedRecord.key());
        byte[] valueBytes = valueJsonConverter.fromConnectData(unwrappedRecord.topic(), unwrappedRecord.valueSchema(), value);

        JsonNode keyRoot = parse(keyBytes);
        JsonNode valueRoot = parse(valueBytes);
        return new IcebergChangeEvent(unwrappedRecord.topic(),
                payloadOf(valueRoot), payloadOf(keyRoot),
                schemaOf(valueRoot), schemaOf(keyRoot),
                configuration, detachedBinary);
    }

    /**
     * A copy of {@code struct} where every plain bytes field (no logical type) holding data is
     * replaced by an empty value, the data itself collected under its dotted path.
     */
    static Struct detachBinary(Struct struct, String path, Map<String, Object> detached) {
        Schema schema = struct.schema();
        Struct copy = null;
        for (Field field : schema.fields()) {
            Object fieldValue = struct.get(field);
            if (fieldValue == null) {
                continue;
            }
            Schema fieldSchema = field.schema();
            String fieldPath = path.isEmpty() ? field.name() : path + "." + field.name();
            Object replacement = null;
            if (fieldSchema.type() == Schema.Type.BYTES && fieldSchema.name() == null) {
                if (fieldValue instanceof byte[] && ((byte[]) fieldValue).length > 0) {
                    detached.put(fieldPath, fieldValue);
                    replacement = EMPTY;
                } else if (fieldValue instanceof ByteBuffer && ((ByteBuffer) fieldValue).remaining() > 0) {
                    detached.put(fieldPath, ((ByteBuffer) fieldValue).slice());
                    replacement = ByteBuffer.wrap(EMPTY);
                }
            } else if (fieldSchema.type() == Schema.Type.STRUCT && fieldValue instanceof Struct) {
                Struct nested = detachBinary((Struct) fieldValue, fieldPath, detached);
                if (nested != fieldValue) {
                    replacement = nested;
                }
            }
            if (replacement != null) {
                if (copy == null) {
                    copy = shallowCopy(struct);
                }
                copy.put(field, replacement);
            }
        }
        return copy == null ? struct : copy;
    }

    private static Struct shallowCopy(Struct struct) {
        Struct copy = new Struct(struct.schema());
        for (Field field : struct.schema().fields()) {
            Object v = struct.get(field);
            if (v != null) {
                copy.put(field, v);
            }
        }
        return copy;
    }

    private static JsonNode parse(byte[] bytes) {
        if (bytes == null) {
            return null;
        }
        try {
            return MAPPER.readTree(bytes);
        } catch (IOException e) {
            throw new ConnectException("Failed to parse record as JSON", e);
        }
    }

    /** The Connect JSON envelope's payload (or the whole document when there is no envelope). */
    private static JsonNode payloadOf(JsonNode root) {
        if (root == null) {
            return null;
        }
        JsonNode payload = root.has("payload") && root.has("schema") ? root.get("payload") : root;
        return payload == null || payload.isNull() ? null : payload;
    }

    private static JsonNode schemaOf(JsonNode root) {
        return Optional.ofNullable(root).map(r -> r.get("schema")).orElse(null);
    }
}
