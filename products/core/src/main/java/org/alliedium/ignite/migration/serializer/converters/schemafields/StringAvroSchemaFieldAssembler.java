package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.serializer.IAvroSchemaBuilder;
import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMeta;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/**
 * Responsible for embedding a meta-data of a String value into avro schema base.
 * Avro schema base is being built within {@link IAvroSchemaBuilder} and needs to be provided as an argument.
 */
class StringAvroSchemaFieldAssembler implements IAvroSchemaFieldAssembler {

    public void assembleAvroSchemaField(FieldAssembler<Schema> fieldAssembler, ICacheFieldMeta fieldMeta) {
        fieldAssembler.name(fieldMeta.getName())
                .type(SchemaBuilder.unionOf().stringType().and().nullType().endUnion()).noDefault();
    }

}
