package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.alliedium.ignite.migration.serializer.IAvroSchemaBuilder;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/**
 * Responsible for embedding a meta-data of a java.sql.Timestamp value into avro schema base.
 * Since the value of java.sql.Timestamp type is being converted when serializing to avro, current assembler defines a new value type in avro schema
 * and leaves a correspondent note (about an initial type) inside the {@link SchemaBuilder.NamedBuilder#doc(String)} attribute for
 * the affected field in avro schema.
 * Avro schema base is being built within {@link IAvroSchemaBuilder} and needs to be provided as an argument.
 *
 * @see <a href="https://avro.apache.org/docs/1.8.2/spec.html#Timestamp+%28millisecond+precision%29">Avro logical type for timestamp-millis</a>
 */
class TimestampAvroSchemaFieldAssembler implements IAvroSchemaFieldAssembler {

    public void assembleAvroSchemaField(FieldAssembler<Schema> fieldAssembler, String fieldName) {
        Schema timestampMilliType = LogicalTypes.timestampMillis().addToSchema(Schema.create(Schema.Type.LONG));
        fieldAssembler.name(fieldName).doc(TypesResolver.getTypeTimestamp()).type(SchemaBuilder.unionOf().type(timestampMilliType).and().nullType().endUnion()).noDefault();
    }

}
