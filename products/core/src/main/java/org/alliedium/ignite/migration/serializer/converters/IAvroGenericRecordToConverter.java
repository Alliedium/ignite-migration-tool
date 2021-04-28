package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.apache.avro.generic.GenericRecord;

/**
 * Provides a functionality of converting provided {@link GenericRecord} to {@link ICacheEntryValue} based on meta-data, which also needs to be provided.
 *
 * @see AvroFieldMetaContainer
 */
public interface IAvroGenericRecordToConverter {

    ICacheEntryValue getCacheEntryValue(GenericRecord avroGenericRecord, AvroFieldMetaContainer avroFieldMetaContainer);

}
