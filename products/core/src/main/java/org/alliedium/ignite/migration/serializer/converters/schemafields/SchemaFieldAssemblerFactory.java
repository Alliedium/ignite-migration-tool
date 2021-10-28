package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMeta;

import java.util.Collections;
import java.util.List;

public class SchemaFieldAssemblerFactory {

    public static IAvroSchemaFieldAssembler get(String type) {
        return get(type, Collections.emptyList());
    }

    public static IAvroSchemaFieldAssembler get(String type, List<ICacheFieldMeta> nested) {

        if (!nested.isEmpty()) {
            return new NestedAvroSchemaFieldAssembler(nested);
        }

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
