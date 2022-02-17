package io.github.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;

class AvroGenericToBeamRowsConverter extends DoFn<GenericRecord, Row> {

    private org.apache.beam.sdk.schemas.Schema schema;

    public AvroGenericToBeamRowsConverter(Schema schema) {
        this.schema = AvroUtils.toBeamSchema(schema);
    }

    @ProcessElement
    public void process(ProcessContext c) {
        c.output(AvroUtils.toBeamRowStrict(c.element(), schema));
    }
}
