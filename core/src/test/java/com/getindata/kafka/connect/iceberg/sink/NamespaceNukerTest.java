package com.getindata.kafka.connect.iceberg.sink;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NamespaceNukerTest {
  private static final Schema SCHEMA = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

  @Test
  void dropsPrefixedTablesAndTheirFilesOnly(@TempDir Path dir) {
    HadoopCatalog catalog = new HadoopCatalog(new Configuration(), dir.toString());
    Namespace ns = Namespace.of("prod");
    catalog.createNamespace(ns);
    Table a = catalog.createTable(TableIdentifier.of(ns, "cdc_a"), SCHEMA);
    Table b = catalog.createTable(TableIdentifier.of(ns, "cdc_b"), SCHEMA);
    Table keep = catalog.createTable(TableIdentifier.of(ns, "manual"), SCHEMA);
    String aDir = a.location().replace("file:", "");
    String bDir = b.location().replace("file:", "");
    String keepDir = keep.location().replace("file:", "");
    assertTrue(new File(aDir).isDirectory());

    Map<String, String> props = new HashMap<>();
    props.put(IcebergSinkConfiguration.TABLE_NAMESPACE, "prod");
    props.put(IcebergSinkConfiguration.TABLE_PREFIX, "cdc_");
    props.put(IcebergSinkConfiguration.NAMESPACE_NUKE, "2026-09-20");
    List<TableIdentifier> dropped = NamespaceNuker.nukeIfRequested(new IcebergSinkConfiguration(props), catalog);

    assertEquals(2, dropped.size());
    assertFalse(catalog.tableExists(TableIdentifier.of(ns, "cdc_a")));
    assertFalse(catalog.tableExists(TableIdentifier.of(ns, "cdc_b")));
    assertTrue(catalog.tableExists(TableIdentifier.of(ns, "manual")));
    assertFalse(new File(aDir).exists());
    assertFalse(new File(bDir).exists());
    assertTrue(new File(keepDir).isDirectory());

    // no token, nothing happens
    props.remove(IcebergSinkConfiguration.NAMESPACE_NUKE);
    assertTrue(NamespaceNuker.nukeIfRequested(new IcebergSinkConfiguration(props), catalog).isEmpty());
    // missing namespace, nothing happens
    props.put(IcebergSinkConfiguration.NAMESPACE_NUKE, "x");
    props.put(IcebergSinkConfiguration.TABLE_NAMESPACE, "nope");
    assertTrue(NamespaceNuker.nukeIfRequested(new IcebergSinkConfiguration(props), catalog).isEmpty());
  }
}
