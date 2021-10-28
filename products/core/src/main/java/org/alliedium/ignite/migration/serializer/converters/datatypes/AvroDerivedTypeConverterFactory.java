package org.alliedium.ignite.migration.serializer.converters.datatypes;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.apache.avro.Schema;

public class AvroDerivedTypeConverterFactory {

    public static IAvroDerivedTypeConverter get(String type) {
        if (TypesResolver.isTimestamp(type)) {
            return new AvroTimestampConverter();
        }
        if (TypesResolver.isByteArray(type)) {
            return new AvroByteArrayConverter();
        }

        return new AvroGenericConverter();
    }

    public static IAvroDerivedTypeConverter get(Schema.Field field) {
        return get(field.doc());
    }
}
