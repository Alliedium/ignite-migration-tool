package org.alliedium.ignite.migration.serializer.converters.schemafields;

import org.alliedium.ignite.migration.dto.ICacheEntryValueField;
import org.alliedium.ignite.migration.serializer.converters.CacheFieldMeta;
import org.alliedium.ignite.migration.serializer.IAvroSchemaBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder.FieldAssembler;

/**
 * Embeds meta-data of a particular field into provided avro schema base.
 * Avro schema base is being built within {@link IAvroSchemaBuilder} and needs to be provided as an argument.
 * Separate instance of current assembler is being created for each {@link ICacheEntryValueField} and
 * included into correspondent {@link CacheFieldMeta}.
 *
 * @see <a href="https://docs.oracle.com/database/nosql-12.1.3.0/GettingStartedGuide/avroschemas.html">Avro schema description</a>
 */
public interface IAvroSchemaFieldAssembler {

    void assembleAvroSchemaField(FieldAssembler<Schema> fieldAssembler, String fieldName);

}