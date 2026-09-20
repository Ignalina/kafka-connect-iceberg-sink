package com.getindata.kafka.connect.iceberg.sink;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.SupportsPrefixOperations;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * {@code table.namespace.nuke=<token>}: at connector start, drop every table of the sink (those whose
 * name starts with {@code table.prefix}) in {@code table.namespace} from the catalog AND delete their
 * data + metadata under the table location in the object store. Kafka offsets are not touched.
 *
 * <p>Nessie ignores {@code purge} on drop (other branches may still reference the files), so the
 * files are removed explicitly through the table's FileIO ({@code S3FileIO.deletePrefix}).
 *
 * <p>The token is recorded as a namespace property once the nuke is done, so the same value left in
 * the configuration does not nuke again on the next restart. Change the token to nuke again.
 */
public final class NamespaceNuker {
  private static final Logger LOGGER = LoggerFactory.getLogger(NamespaceNuker.class);
  static final String TOKEN_PROPERTY = "kafka-connect-iceberg-sink.nuke-token";

  private NamespaceNuker() {
  }

  /** @return the identifiers that were dropped (empty when nothing was requested or nothing was there) */
  public static List<TableIdentifier> nukeIfRequested(IcebergSinkConfiguration configuration, Catalog catalog) {
    String token = configuration.getNamespaceNuke();
    if (token == null || token.trim().isEmpty()) {
      return Collections.emptyList();
    }
    token = token.trim();
    Namespace namespace = Namespace.of(configuration.getTableNamespace());
    if (!(catalog instanceof SupportsNamespaces)) {
      throw new ConfigException(IcebergSinkConfiguration.NAMESPACE_NUKE, token,
          "catalog " + catalog.getClass().getName() + " does not support namespaces");
    }
    SupportsNamespaces namespaces = (SupportsNamespaces) catalog;
    if (!namespaces.namespaceExists(namespace)) {
      LOGGER.warn("NUKE requested with token '{}' but namespace '{}' does not exist, nothing to do", token, namespace);
      return Collections.emptyList();
    }
    Map<String, String> properties;
    try {
      properties = namespaces.loadNamespaceMetadata(namespace);
    } catch (NoSuchNamespaceException | UnsupportedOperationException e) {
      properties = Collections.emptyMap();
    }
    if (token.equals(properties.get(TOKEN_PROPERTY))) {
      LOGGER.info("NUKE token '{}' already applied to namespace '{}', skipping (change the token to nuke again)",
          token, namespace);
      return Collections.emptyList();
    }

    String prefix = configuration.getTablePrefix() == null ? "" : configuration.getTablePrefix();
    List<TableIdentifier> dropped = new ArrayList<>();
    LOGGER.warn("NUKE token '{}': dropping every table '{}*' in namespace '{}' from the catalog and the object store",
        token, prefix, namespace);
    for (TableIdentifier id : catalog.listTables(namespace)) {
      if (!id.name().startsWith(prefix)) {
        LOGGER.info("NUKE keeps '{}' (no '{}' prefix)", id, prefix);
        continue;
      }
      String location = null;
      FileIO io = null;
      try {
        Table table = catalog.loadTable(id);
        location = table.location();
        io = table.io();
      } catch (RuntimeException e) {
        LOGGER.warn("NUKE could not load '{}' to find its location, dropping it from the catalog only: {}", id, e.toString());
      }
      boolean gone = catalog.dropTable(id, true);
      LOGGER.warn("NUKE dropped '{}' from the catalog: {}", id, gone);
      if (location != null) {
        if (io instanceof SupportsPrefixOperations) {
          ((SupportsPrefixOperations) io).deletePrefix(location);
          LOGGER.warn("NUKE deleted everything under {}", location);
        } else {
          LOGGER.warn("NUKE could not delete files under {}: FileIO {} has no prefix delete, remove them by hand",
              location, io == null ? "null" : io.getClass().getName());
        }
      }
      dropped.add(id);
    }

    try {
      namespaces.setProperties(namespace, Collections.singletonMap(TOKEN_PROPERTY, token));
    } catch (UnsupportedOperationException e) {
      LOGGER.warn("NUKE done but catalog {} cannot store the token on the namespace: REMOVE {} from the "
          + "configuration before the next restart or it nukes again", catalog.getClass().getName(),
          IcebergSinkConfiguration.NAMESPACE_NUKE);
    }
    LOGGER.warn("NUKE done: {} tables dropped in namespace '{}'", dropped.size(), namespace);
    return dropped;
  }
}
