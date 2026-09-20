package com.getindata.kafka.connect.iceberg.sink.tableoperator;

import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.data.InternalRecordWrapper;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.FileAppenderFactory;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.Sets;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.getindata.kafka.connect.iceberg.sink.tableoperator.CdcOperation.CREATE;
import static com.getindata.kafka.connect.iceberg.sink.tableoperator.CdcOperation.DELETE;

public abstract class BaseDeltaTaskWriter extends BaseTaskWriter<Record> {

    private static final Logger LOGGER = LoggerFactory.getLogger(BaseDeltaTaskWriter.class);
    /** After the first occurrence per identifier column, the null-key warning is repeated every this many rows. */
    static final long NULL_KEY_WARN_EVERY = 10_000L;

    private final Schema schema;
    private final Schema deleteSchema;
    private final InternalRecordWrapper wrapper;

    private final InternalRecordWrapper keyWrapper;
    private final boolean upsert;
    private final boolean upsertKeepDeletes;
    private final String tableName;
    /** The equality (identifier) columns in schema order; an equality delete needs a value for every one. */
    private final List<Types.NestedField> equalityFields;
    /** Rows seen with a null value per identifier column, for rate-limiting the warning. */
    private final Map<String, Long> nullKeyCounts = new HashMap<>();

    public BaseDeltaTaskWriter(PartitionSpec spec,
                               FileFormat format,
                               FileAppenderFactory<Record> appenderFactory,
                               OutputFileFactory fileFactory,
                               FileIO io,
                               long targetFileSize,
                               Schema schema,
                               List<Integer> equalityFieldIds,
                               boolean upsert,
                               boolean upsertKeepDeletes) {
        this(spec, format, appenderFactory, fileFactory, io, targetFileSize, schema, equalityFieldIds, upsert,
                upsertKeepDeletes, null);
    }

    public BaseDeltaTaskWriter(PartitionSpec spec,
                               FileFormat format,
                               FileAppenderFactory<Record> appenderFactory,
                               OutputFileFactory fileFactory,
                               FileIO io,
                               long targetFileSize,
                               Schema schema,
                               List<Integer> equalityFieldIds,
                               boolean upsert,
                               boolean upsertKeepDeletes,
                               String tableName) {
        super(spec, format, appenderFactory, fileFactory, io, targetFileSize);
        this.schema = schema;
        this.deleteSchema = TypeUtil.select(schema, Sets.newHashSet(equalityFieldIds));
        this.wrapper = new InternalRecordWrapper(schema.asStruct());
        this.keyWrapper = new InternalRecordWrapper(deleteSchema.asStruct());
        this.upsert = upsert;
        this.upsertKeepDeletes = upsertKeepDeletes;
        this.tableName = tableName != null ? tableName : "schema " + schema.asStruct() + " partition " + spec;
        this.equalityFields = new ArrayList<>();
        for (Integer id : equalityFieldIds) {
            Types.NestedField field = schema.findField(id);
            if (field != null) {
                this.equalityFields.add(field);
            }
        }
    }

    abstract RowDataDeltaWriter route(Record row);

    InternalRecordWrapper wrapper() {
        return wrapper;
    }

    @Override
    public void write(Record row) throws IOException {
        // Identifier columns are required in an Iceberg schema (Schema refuses an optional identifier
        // field), so a null there cannot be written anywhere in this table: neither as the equality
        // delete (which carries only the identifier columns) nor as the data row. Both die with an NPE
        // in the Parquet column writer and the task with them. There is no row identity to act on, so
        // the row is skipped and counted, whatever its op.
        List<String> nullKeys = nullIdentifierColumns(row);
        Object op = row.getField("__op");
        if (!nullKeys.isEmpty()) {
            warnNullIdentifier(nullKeys, op);
            return;
        }
        RowDataDeltaWriter writer = route(row);
        boolean isCreate = CREATE.getCode().equals(op);
        boolean isDelete = DELETE.getCode().equals(op);
        if (upsert && !isCreate) {// anything which not an insert is upsert
            writer.delete(row);
        }
        // if its deleted row and upsertKeepDeletes = true then add deleted record to target table
        // else deleted records are deleted from target table
        if (upsertKeepDeletes || !isDelete) {
            writer.write(row);
        }
    }

    /** The identifier columns whose value is null in this row, in schema order; empty when the key is complete. */
    private List<String> nullIdentifierColumns(Record row) {
        List<String> nulls = null;
        for (Types.NestedField field : equalityFields) {
            if (row.getField(field.name()) == null) {
                if (nulls == null) {
                    nulls = new ArrayList<>(equalityFields.size());
                }
                nulls.add(field.name());
            }
        }
        return nulls == null ? List.of() : nulls;
    }

    /** Logs the first occurrence per identifier column, then every {@link #NULL_KEY_WARN_EVERY}th. */
    private void warnNullIdentifier(List<String> nullKeys, Object op) {
        for (String column : nullKeys) {
            long seen = nullKeyCounts.merge(column, 1L, Long::sum);
            if (seen == 1L || seen % NULL_KEY_WARN_EVERY == 0L) {
                LOGGER.warn("Table {}: identifier column '{}' is null in a row with op '{}' ({} such rows so far); "
                                + "an identifier column is required so the row can neither be equality-deleted nor "
                                + "written, it is skipped",
                        tableName, column, op, seen);
            }
        }
    }

    /** Rows seen with a null value in the given identifier column (for tests and diagnostics). */
    long nullKeyCount(String column) {
        return nullKeyCounts.getOrDefault(column, 0L);
    }

    public class RowDataDeltaWriter extends BaseEqualityDeltaWriter {
        RowDataDeltaWriter(PartitionKey partition) {
            super(partition, schema, deleteSchema);
        }

        @Override
        protected StructLike asStructLike(Record data) {
            return wrapper.wrap(data);
        }

        @Override
        protected StructLike asStructLikeKey(Record data) {
            return keyWrapper.wrap(data);
        }
    }
}
