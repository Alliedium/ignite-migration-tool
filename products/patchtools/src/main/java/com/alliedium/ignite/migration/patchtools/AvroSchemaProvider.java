package com.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;

public interface AvroSchemaProvider {
    Schema getSchema();
}
