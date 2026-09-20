package com.getindata.kafka.connect.iceberg.sink;

import com.getindata.kafka.connect.iceberg.sink.converter.SinkRecordToIcebergChangeEventConverter;
import com.getindata.kafka.connect.iceberg.sink.converter.SinkRecordToIcebergChangeEventConverterFactory;
import com.getindata.kafka.connect.iceberg.sink.tableoperator.IcebergTableOperator;
import com.getindata.kafka.connect.iceberg.sink.tableoperator.IcebergTableOperatorFactory;
import org.apache.iceberg.catalog.Catalog;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * The sink task, with a byte-budgeted flush buffer.
 *
 * <p>Records are buffered across {@code put()} calls and written to Iceberg (one commit per table)
 * when the buffer reaches {@code flush.size} records or {@code flush.max-bytes} bytes, or when
 * Kafka Connect asks for the offsets to commit ({@code preCommit}, every
 * {@code offset.flush.interval.ms}), so a rebalance never loses buffered rows. The number of Kafka
 * polls no longer decides the Iceberg file size: {@code consumer.override.max.poll.records} can be
 * large for small rows while {@code flush.max-bytes} keeps the memory bounded for big ones.
 */
public class IcebergSinkTask extends SinkTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergSinkTask.class);

    private Consumer<Collection<SinkRecord>> consumer;
    private int flushSize;
    private long flushMaxBytes;

    private final List<SinkRecord> pending = new ArrayList<>();
    private long pendingBytes;
    private long pendingSince;

    @Override
    public String version() {
        return IcebergSinkVersion.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        LOGGER.info("Task starting");
        IcebergSinkConfiguration configuration = new IcebergSinkConfiguration(properties);
        Catalog icebergCatalog = IcebergCatalogFactory.create(configuration);
        IcebergTableOperator icebergTableOperator = IcebergTableOperatorFactory.create(configuration);
        SinkRecordToIcebergChangeEventConverter converter = SinkRecordToIcebergChangeEventConverterFactory.create(configuration);
        IcebergChangeConsumer changeConsumer = new IcebergChangeConsumer(configuration, icebergCatalog, icebergTableOperator, converter);
        start(changeConsumer::accept, configuration);
    }

    /** Wire the task to any consumer of record batches (tests). */
    void start(Consumer<Collection<SinkRecord>> consumer, IcebergSinkConfiguration configuration) {
        this.consumer = consumer;
        this.flushSize = configuration.getFlushSize();
        this.flushMaxBytes = configuration.getFlushMaxBytes();
        LOGGER.info("Flush buffer: {} records or {} bytes, or on offset commit", flushSize, flushMaxBytes);
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        LOGGER.debug("Received {} records", records.size());
        for (SinkRecord record : records) {
            if (pending.isEmpty()) {
                pendingSince = System.currentTimeMillis();
            }
            pending.add(record);
            pendingBytes += SinkRecordSizer.estimate(record);
            if (pending.size() >= flushSize || pendingBytes >= flushMaxBytes) {
                flushPending("size");
            }
        }
    }

    @Override
    public Map<TopicPartition, OffsetAndMetadata> preCommit(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        // everything delivered so far is written before its offsets are committed
        flushPending("offset commit");
        return currentOffsets;
    }

    @Override
    public void close(Collection<TopicPartition> partitions) {
        flushPending("partitions closed");
    }

    @Override
    public void stop() {
        LOGGER.info("Task stopped");
    }

    int pendingRecords() {
        return pending.size();
    }

    long pendingBytes() {
        return pendingBytes;
    }

    private void flushPending(String reason) {
        if (pending.isEmpty()) {
            return;
        }
        long start = System.currentTimeMillis();
        int count = pending.size();
        long bytes = pendingBytes;
        try {
            consumer.accept(pending);
        } finally {
            // on failure the task fails and restarts from the last committed offsets; the buffer is not reused
            pending.clear();
            pendingBytes = 0;
        }
        LOGGER.info("Flushed {} records (~{} bytes) buffered for {} ms, trigger: {}, write took {} ms",
                count, bytes, start - pendingSince, reason, System.currentTimeMillis() - start);
        pendingSince = 0;
    }
}
