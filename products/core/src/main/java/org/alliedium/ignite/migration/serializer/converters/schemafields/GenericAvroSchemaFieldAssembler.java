package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.serializer.IAvroSchemaBuilder;
import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMeta;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/**
 * Non-functional assembler. Used for embedding meta-data of DTO fields, which value types are supported in avro.
 * Value type class name needs to be provided when initializing the assembler.
 * Avro schema base is being built within {@link IAvroSchemaBuilder} and needs to be provided as an argument.
 */
public class GenericAvroSchemaFieldAssembler implements IAvroSchemaFieldAssembler {

    private final String initialFieldTypeClassName;

    public GenericAvroSchemaFieldAssembler(String initialFieldTypeClassName) {
        this.initialFieldTypeClassName = initialFieldTypeClassName;
    }

    public void assembleAvroSchemaField(FieldAssembler<Schema> fieldAssembler, ICacheFieldMeta fieldMeta) {
        fieldAssembler.name(fieldMeta.getName()).type(
                SchemaBuilder.unionOf().type(this.initialFieldTypeClassName.toLowerCase()).and().nullType().endUnion()).noDefault();
    }

}
