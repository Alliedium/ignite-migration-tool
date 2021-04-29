package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.alliedium.ignite.migration.serializer.IAvroSchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/**
 * Responsible for embedding a meta-data of a byte-array value into avro schema base.
 * Since the value of byte-array type is being converted when serializing to avro, current assembler defines a new value type in avro schema
 * and leaves a correspondent note (about an initial type) inside the {@link SchemaBuilder.NamedBuilder#doc(String)} attribute for
 * the affected field in avro schema.
 * Avro schema base is being built within {@link IAvroSchemaBuilder} and needs to be provided as an argument.
 */
class ByteArrayAvroSchemaFieldAssembler implements
    IAvroSchemaFieldAssembler { //TODO: byte-array values need to be serialized via native avro mechanisms, once it is done - current assembler needs to either be deleted or updated

    public void assembleAvroSchemaField(FieldAssembler<Schema> fieldAssembler, String fieldName) {
        fieldAssembler.name(fieldName).doc(TypesResolver.getTypeByteArray()).type(SchemaBuilder.unionOf().stringType().and().nullType().endUnion()).noDefault();
    }

}