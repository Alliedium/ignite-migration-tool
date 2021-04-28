package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMetaContainer;
import java.util.List;

import org.alliedium.ignite.migration.serializer.converters.schemafields.IAvroSchemaFieldAssembler;
import org.apache.avro.Schema;

/**
 * Provides mechanisms for avro schema creation.
 * Separate avro schema is required for both pieces. Current interface provides methods capable for both avro schema types creation.
 * Stored cache data may contain types, which are not supported in avro.
 * These should be followed with correspondent {@link IAvroSchemaFieldAssembler}. Mentioned assemblers need to be predefined and provided
 * inside the {@link ICacheFieldMetaContainer}.
 */
public interface IAvroSchemaBuilder {

    Schema getCacheDataAvroSchema(List<String> cacheValueFieldNamesList, ICacheFieldMetaContainer converterContainer);

    Schema getCacheConfigurationsAvroSchema();

    Schema getAtomicStructureSchema();
}
