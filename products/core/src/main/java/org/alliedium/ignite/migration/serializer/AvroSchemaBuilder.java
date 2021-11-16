package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMeta;
import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMetaContainer;

import java.util.*;

import org.alliedium.ignite.migration.serializer.utils.FieldNames;
import org.alliedium.ignite.migration.util.UniqueKey;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

/**
 * AvroSchemaBuilder is responsible for avro schema creation. Avro format intends an avro schema presence for each avro file.
 * Hence current AvroSchemaBuilder unit is able to create avro schemas for both configurations and data avro files.
 * When the derived data type is not supported by avro, converter needs to be applied.
 * Converters are taken from correspondent {@link ICacheFieldMetaContainer} containers which need to be passed from the outside.
 *
 * @see <a href="https://docs.oracle.com/database/nosql-12.1.3.0/GettingStartedGuide/avroschemas.html">Avro schema description</a>
 */
public class AvroSchemaBuilder implements IAvroSchemaBuilder {

    private static final String CACHE_CONFIGURATIONS_RECORD_NAME = "CacheConfigurations";
    private static final String CONFIG_NAMESPACE = "configs";
    private static final String ATOMIC_NAMESPACE = "atomics";
    private static final String ATOMIC_STRUCTURE_RECORD_NAME = "AtomicStructure";

    private static final String CACHE_CONFIGURATIONS_FIELD_NAME = "cacheConfigurations";
    private static final String CACHE_QUERY_ENTITIES_FIELD_NAME = "cacheQueryEntities";

    @Override
    public Schema getCacheConfigurationsAvroSchema() {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(CACHE_CONFIGURATIONS_RECORD_NAME)
                .namespace(CONFIG_NAMESPACE).fields();

        fieldAssembler.name(CACHE_CONFIGURATIONS_FIELD_NAME).type().stringType().noDefault();
        fieldAssembler.name(CACHE_QUERY_ENTITIES_FIELD_NAME).type().stringType().noDefault();

        return fieldAssembler.endRecord();
    }

    @Override
    public Schema getAtomicStructureSchema() {
        SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder.record(ATOMIC_STRUCTURE_RECORD_NAME)
                .namespace(ATOMIC_NAMESPACE).fields();
        fieldAssembler
                .name(FieldNames.IGNITE_ATOMIC_LONG_NAME_FIELD_NAME)
                .type(SchemaBuilder.unionOf().stringType().and().nullType().endUnion()).noDefault();
        fieldAssembler
                .name(FieldNames.IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME)
                .type(SchemaBuilder.unionOf().longType().and().nullType().endUnion()).noDefault();

        return fieldAssembler.endRecord();
    }

    @Override
    public Schema getCacheDataAvroSchema(Schema keySchema, List<String> fieldNames, ICacheFieldMetaContainer converterContainer) {
        final SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                .record(UniqueKey.generate())
                .fields();

        fieldAssembler.name(FieldNames.KEY_FIELD_NAME).type(keySchema).noDefault();

        setAvroFields(fieldNames, converterContainer, fieldAssembler);

        return fieldAssembler.endRecord();
    }

    @Override
    public Schema getSchemaForFields(List<String> fieldNames, ICacheFieldMetaContainer converterContainer) {
        final SchemaBuilder.FieldAssembler<Schema> fieldAssembler = SchemaBuilder
                .record(UniqueKey.generate())
                .fields();

        setAvroFields(fieldNames, converterContainer, fieldAssembler);

        return fieldAssembler.endRecord();
    }

    private void setAvroFields(List<String> fieldNames, ICacheFieldMetaContainer converterContainer,
                                                               SchemaBuilder.FieldAssembler<Schema> fieldAssembler) {
        for (String fieldName : fieldNames) {
            ICacheFieldMeta fieldMeta = converterContainer.getFieldTypeMeta(fieldName);
            fieldMeta.getAvroSchemaFieldAssembler().assembleAvroSchemaField(fieldAssembler, fieldMeta);
        }
    }
}
