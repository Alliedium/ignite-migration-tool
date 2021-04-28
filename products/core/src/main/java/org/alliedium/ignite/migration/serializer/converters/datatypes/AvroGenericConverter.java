package org.alliedium.ignite.migration.serializer.converters.datatypes;

import org.alliedium.ignite.migration.serializer.converters.CacheFieldMeta;

/**
 * Non-functional container. Used for DTO data of types, which are supported in avro.
 * Current converter in needed for {@link CacheFieldMeta} initialization,
 * which is being built for each data field derived from DTO.
 */
class AvroGenericConverter implements IAvroDerivedTypeConverter {

    public Object convertForAvro(Object fieldData) {
        return fieldData;
    }

    public Object convertFromAvro(Object fieldData) {
        return fieldData;
    }

}
