package org.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

class BeamRowsToAvroConverter extends DoFn<Row, GenericRecord> {

    private final Schema schema;

    public BeamRowsToAvroConverter(Schema schema) {
        this.schema = schema;
    }

    @ProcessElement
    public void process(ProcessContext c) {
        c.output(AvroUtils.toGenericRecord(c.element(), schema));
    }
}
