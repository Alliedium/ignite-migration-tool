package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.serializer.IAvroSchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/**
 * Responsible for embedding a meta-data of a Integer value into avro schema base.
 * Avro schema base is being built within {@link IAvroSchemaBuilder} and needs to be provided as an argument.
 */
class IntegerAvroSchemaFieldAssembler implements IAvroSchemaFieldAssembler {

    public void assembleAvroSchemaField(FieldAssembler<Schema> fieldAssembler, String fieldName) {
        fieldAssembler.name(fieldName).type(SchemaBuilder.unionOf().intType().and().nullType().endUnion()).noDefault();
    }

}