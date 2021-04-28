package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;

public class SchemaFieldAssemblerFactory {

    public static IAvroSchemaFieldAssembler get(String type) {
        if (TypesResolver.isTimestamp(type)) {
            return new TimestampAvroSchemaFieldAssembler();
        }
        if (TypesResolver.isByteArray(type)) {
            return new ByteArrayAvroSchemaFieldAssembler();
        }
        if (TypesResolver.isString(type)) {
            return new StringAvroSchemaFieldAssembler();
        }
        if (TypesResolver.isInteger(type)) {
            return new IntegerAvroSchemaFieldAssembler();
        }

        return new GenericAvroSchemaFieldAssembler(type);
    }
}
