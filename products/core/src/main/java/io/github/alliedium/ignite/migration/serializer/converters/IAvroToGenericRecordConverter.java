package io.github.alliedium.ignite.migration.serializer.converters;

import io.github.alliedium.ignite.migration.dto.ICacheEntryValue;
import org.apache.avro.generic.GenericRecord;

import java.util.Set;

/**
 * Provides a functionality of converting provided {@link GenericRecord} to {@link ICacheEntryValue} based on meta-data, which also needs to be provided.
 *
 * @see AvroFieldMetaContainer
 */
public interface IAvroToGenericRecordConverter {

    ICacheEntryValue getCacheEntryValue(GenericRecord avroGenericRecord);

    ICacheEntryValue getCacheEntryValue(GenericRecord avroGenericRecord, Set<String> fieldNamesToIgnore);

}
