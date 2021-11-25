package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.io.AvroIO;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.beam.sdk.schemas.transforms.Select.fieldNames;

public class SelectAction implements TransformAction<TransformOutput> {
    private static final IIgniteDTOConverter<String, Collection<QueryEntity>> queryEntityConverter =
            IgniteObjectStringConverter.QUERY_ENTITY_CONVERTER;
    private static final IIgniteDTOConverter<String, CacheConfiguration<Object, BinaryObject>> cacheConfigConverter =
            IgniteObjectStringConverter.CACHE_CONFIG_CONVERTER;

    private final String[] fields;
    private final String from;
    private final PatchContext context;

    private SelectAction(Builder builder) {
        context = builder.context;
        from = builder.from;
        fields = builder.fields;
    }

    public TransformOutput execute() {
        CacheComponent cacheComponent = context.getCacheComponent(from);

        Schema schema = cacheComponent.getAvroFileReader().getCacheDataAvroSchema();
        PCollection<GenericRecord> records = context.getPipeline().apply(AvroIO.readGenericRecords(schema)
                .withBeamSchemas(true)
                .from(cacheComponent.getFilesLocator().cacheDataPath().toString()));
        PCollection<Row> rows = records.apply(fieldNames(fields));

        Schema updatedSchema = Util.filterSchemaFields(schema, fields);

        CacheConfiguration<Object, BinaryObject> cacheConfiguration;
        Collection<QueryEntity> queryEntities;

        cacheConfiguration = cacheConfigConverter
                .convertFromDTO(cacheComponent.getAvroFileReader().getCacheConfiguration());
        queryEntities = queryEntityConverter
                .convertFromDTO(cacheComponent.getAvroFileReader().getCacheEntryMeta());

        queryEntities = queryEntities.stream()
                .map(queryEntity -> Util.filterQueryEntityFields(queryEntity, fields))
                .map(queryEntity -> Util.filterQueryEntityIndexes(queryEntity, fields))
                .collect(Collectors.toList());
        cacheConfiguration.setQueryEntities(queryEntities);

        return new TransformOutput.Builder()
                .setPCollection(rows)
                .setFields(fields)
                .setSchema(updatedSchema)
                .setCacheComponents(cacheComponent)
                .setCacheConfiguration(cacheConfiguration)
                .setQueryEntities(queryEntities)
                .build();
    }

    public static class Builder {
        private String[] fields;
        private String from;
        private PatchContext context;

        public Builder context(PatchContext context) {
            this.context = context;
            return this;
        }

        public Builder fields(String... fields) {
            Set<String> fieldsSet = Stream.of(fields).collect(Collectors.toSet());
            this.fields = fieldsSet.toArray(new String[0]);
            return this;
        }

        public Builder from(String from) {
            this.from = from;
            return this;
        }

        private void validate() {
            Objects.requireNonNull(context, "Patch context is not set");
            Objects.requireNonNull(fields, "no fields set");
            Objects.requireNonNull(from, "from is not set");
        }

        public SelectAction build() {
            validate();
            return new SelectAction(this);
        }
    }
}
