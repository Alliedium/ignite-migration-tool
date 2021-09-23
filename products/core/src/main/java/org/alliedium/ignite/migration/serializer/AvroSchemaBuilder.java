package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.serializer.converters.ICacheFieldMetaContainer;
import java.util.List;

import org.alliedium.ignite.migration.serializer.utils.FieldNames;
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
    private static final String RECORDS_NAMESPACE = "test";
    private static final String ATOMIC_STRUCTURE_RECORD_NAME = "AtomicStructure";

    private static final String CACHE_CONFIGURATIONS_FIELD_NAME = "cacheConfigurations";
    private static final String CACHE_QUERY_ENTITIES_FIELD_NAME = "cacheQueryEntities";
    private static final String CACHE_DATA_RECORD_NAME = "CacheEntry";
    private static final String CACHE_DATA_RECORD_NAMESPACE = "test";

    private static final String IGNITE_ATOMIC_LONG_NAME_FIELD_NAME = "igniteAtomicLongName";
    private static final String IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME = "igniteAtomicLongValue";

    @Override
    public Schema getCacheConfigurationsAvroSchema() {
        SchemaBuilder.FieldAssembler<Schema> cacheConfigurationsAvroSchemaAssembly = SchemaBuilder.record(CACHE_CONFIGURATIONS_RECORD_NAME)
                .namespace(RECORDS_NAMESPACE).fields();

        cacheConfigurationsAvroSchemaAssembly.name(CACHE_CONFIGURATIONS_FIELD_NAME).type().stringType().noDefault();
        cacheConfigurationsAvroSchemaAssembly.name(CACHE_QUERY_ENTITIES_FIELD_NAME).type().stringType().noDefault();

        return cacheConfigurationsAvroSchemaAssembly.endRecord();
    }

    @Override
    public Schema getAtomicStructureSchema() {
        SchemaBuilder.FieldAssembler<Schema> atomicStructureFieldAssembler = SchemaBuilder.record(ATOMIC_STRUCTURE_RECORD_NAME)
                .namespace(RECORDS_NAMESPACE).fields();
        atomicStructureFieldAssembler
                .name(IGNITE_ATOMIC_LONG_NAME_FIELD_NAME)
                .type(SchemaBuilder.unionOf().stringType().and().nullType().endUnion()).noDefault();
        atomicStructureFieldAssembler
                .name(IGNITE_ATOMIC_LONG_VALUE_FIELD_NAME)
                .type(SchemaBuilder.unionOf().longType().and().nullType().endUnion()).noDefault();

        return atomicStructureFieldAssembler.endRecord();
    }

    @Override
    public Schema getCacheDataAvroSchema(List<String> cacheValueFieldNamesList, ICacheFieldMetaContainer converterContainer) {
        final SchemaBuilder.FieldAssembler<Schema> cacheDataAvroSchemaAssembly = SchemaBuilder.record(CACHE_DATA_RECORD_NAME)
            .namespace(CACHE_DATA_RECORD_NAMESPACE).fields();

        cacheDataAvroSchemaAssembly.name(FieldNames.AVRO_GENERIC_RECORD_KEY_FIELD_NAME)
                .type(SchemaBuilder.unionOf().stringType().and().nullType().endUnion()).noDefault();

        for (String cacheValueFieldName : cacheValueFieldNamesList) {
            converterContainer.getFieldTypeMeta(cacheValueFieldName).getAvroSchemaFieldAssembler().assembleAvroSchemaField(cacheDataAvroSchemaAssembly, cacheValueFieldName);
        }

        return cacheDataAvroSchemaAssembly.endRecord();
    }

}
