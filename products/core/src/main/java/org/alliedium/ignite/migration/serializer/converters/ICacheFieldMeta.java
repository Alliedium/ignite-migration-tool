package org.alliedium.ignite.migration.serializer.converters;

import org.alliedium.ignite.migration.dto.CacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.datatypes.IAvroDerivedTypeConverter;
import org.alliedium.ignite.migration.serializer.converters.schemafields.IAvroSchemaFieldAssembler;

/**
 * Responsible for storing and returning all the meta-data of
 * separate {@link CacheEntryValueField}, required for this field to be serialized to avro.
 *
 * @see IAvroDerivedTypeConverter
 * @see IAvroSchemaFieldAssembler
 */
public interface ICacheFieldMeta {

    String getName();

    IAvroSchemaFieldAssembler getAvroSchemaFieldAssembler();

    IAvroDerivedTypeConverter getAvroDerivedTypeConverter();

}
