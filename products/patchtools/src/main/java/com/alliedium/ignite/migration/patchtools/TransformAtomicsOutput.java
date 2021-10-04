package com.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;

public class TransformAtomicsOutput {
    private final PCollection<Row> pCollection;
    private final Schema schema;

    public TransformAtomicsOutput(PCollection<Row> pCollection, Schema schema) {
        this.pCollection = pCollection;
        this.schema = schema;
    }

    public PCollection<Row> getPCollection() {
        return pCollection;
    }

    public Schema getSchema() {
        return schema;
    }
}
