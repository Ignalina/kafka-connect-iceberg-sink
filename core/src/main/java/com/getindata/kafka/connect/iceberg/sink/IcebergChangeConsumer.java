package com.getindata.kafka.connect.iceberg.sink;

import com.getindata.kafka.connect.iceberg.sink.converter.SinkRecordToIcebergChangeEventConverter;
import com.getindata.kafka.connect.iceberg.sink.tableoperator.IcebergTableOperator;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.getindata.kafka.connect.iceberg.sink.IcebergSinkConfiguration.TABLE_AUTO_CREATE;

public class IcebergChangeConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(IcebergChangeConsumer.class);

    private final IcebergSinkConfiguration configuration;
    private final Catalog icebergCatalog;
    private final IcebergTableOperator icebergTableOperator;
    private final SinkRecordToIcebergChangeEventConverter converter;
    private final Set<Namespace> ensuredNamespaces = ConcurrentHashMap.newKeySet();

    public IcebergChangeConsumer(IcebergSinkConfiguration configuration,
                                 Catalog icebergCatalog,
                                 IcebergTableOperator icebergTableOperator,
                                 SinkRecordToIcebergChangeEventConverter converter) {
        this.configuration = configuration;
        this.icebergCatalog = icebergCatalog;
        this.icebergTableOperator = icebergTableOperator;
        this.converter = converter;
    }

    public void accept(Collection<SinkRecord> records) {
        Instant start = Instant.now();

        Map<String, List<IcebergChangeEvent>> result = records.stream()
                .map(converter::convert)
                .filter(Objects::nonNull)
                .collect(Collectors.groupingBy(IcebergChangeEvent::destinationTable));

        for (Map.Entry<String, List<IcebergChangeEvent>> event : result.entrySet()) {
            String eventKey = event.getKey();

            if (configuration.isTableSnakeCase()) {
                eventKey = IcebergUtil.toSnakeCase(eventKey);
            }
            String tableName = configuration.getTablePrefix() + eventKey;
            TableSettings settings = configuration.forTable(tableName);
            TableIdentifier tableIdentifier = TableIdentifier.of(Namespace.of(configuration.getTableNamespace()), tableName);
            Table icebergTable = loadIcebergTable(icebergCatalog, tableIdentifier, event.getValue().get(0), settings);
            icebergTableOperator.addToTable(icebergTable, event.getValue(), settings);
        }

        Instant end = Instant.now();
        LOGGER.debug("Processed {} records in {} ms", records.size(), ChronoUnit.MILLIS.between(start, end));
    }

    private Table loadIcebergTable(Catalog icebergCatalog, TableIdentifier tableId, IcebergChangeEvent sampleEvent, TableSettings settings) {
        return IcebergUtil.loadIcebergTable(icebergCatalog, tableId).orElseGet(() -> {
            if (!configuration.isTableAutoCreate()) {
                throw new ConnectException(String.format("Table '%s' not found! Set '%s' to true to create tables automatically!", tableId, TABLE_AUTO_CREATE));
            }
            ensureNamespace(tableId.namespace());
            LOGGER.info("Table '{}' settings: {}", tableId, settings);
            return IcebergUtil.createIcebergTable(icebergCatalog, tableId, sampleEvent.icebergSchema(settings.getPartitionColumn()), settings);
        });
    }

    /**
     * Create the namespace before the first table is auto-created in it. Catalogs such as Nessie refuse
     * to create a table in a namespace that does not exist yet.
     */
    private void ensureNamespace(Namespace namespace) {
        if (!configuration.isNamespaceAutoCreate() || ensuredNamespaces.contains(namespace)) {
            return;
        }
        if (!(icebergCatalog instanceof SupportsNamespaces)) {
            LOGGER.warn("Catalog {} does not support namespaces, cannot auto-create namespace '{}'",
                    icebergCatalog.getClass().getName(), namespace);
            ensuredNamespaces.add(namespace);
            return;
        }
        SupportsNamespaces namespaceCatalog = (SupportsNamespaces) icebergCatalog;
        if (!namespaceCatalog.namespaceExists(namespace)) {
            LOGGER.info("Namespace '{}' not found, creating it", namespace);
            try {
                namespaceCatalog.createNamespace(namespace);
            } catch (AlreadyExistsException e) {
                LOGGER.debug("Namespace '{}' was created concurrently", namespace);
            }
        }
        ensuredNamespaces.add(namespace);
    }
}
