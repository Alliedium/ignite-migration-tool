package org.alliedium.ignite.migration.serializer.converters.datatypes;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;

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
}
