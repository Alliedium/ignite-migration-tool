package io.github.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

public class CopyFieldAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> action;
    private final String fieldToCopy;
    private final String newFieldName;

    private CopyFieldAction(Builder builder) {
        this.action = builder.action;
        this.fieldToCopy = builder.fieldToCopy;
        this.newFieldName = builder.newFieldName;
    }

    public TransformOutput execute() {
        TransformOutput out = action.execute();
        Set<String> fields = Arrays.stream(out.getFields()).collect(Collectors.toSet());
        fields.add(newFieldName);

        Schema schema = out.getSchema();
        List<Schema.Field> schemaFields = new ArrayList<>(schema.getFields());

        Schema.Field oldSchemaField = Util.searchFieldByName(out, fieldToCopy);
        Schema.Field newSchemaField = new Schema.Field(newFieldName, oldSchemaField.schema(),
                oldSchemaField.doc(), oldSchemaField.defaultVal(), oldSchemaField.order());

        Set<Schema.Field> newSchemaFields = schemaFields.stream()
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
                .collect(Collectors.toSet());

        newSchemaFields.add(newSchemaField);

        Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(),
                schema.getNamespace(), false, new ArrayList<>(newSchemaFields));

        org.apache.beam.sdk.schemas.Schema beamSchema = AvroUtils.toBeamSchema(newSchema);

        PCollection<Row> pCollection = out.getPCollection()
                .apply(ParDo.of(CopyFieldProcessor.create(beamSchema, fieldToCopy, newFieldName)))
                .setRowSchema(beamSchema)
                .setCoder(SchemaCoder.of(beamSchema));

        CacheConfiguration<Object, BinaryObject> cacheConfiguration = out.getCacheConfiguration();
        Collection<QueryEntity> queryEntities = out.getQueryEntities();
        queryEntities.forEach(queryEntity -> QueryEntityUtils.copyField(queryEntity, fieldToCopy, newFieldName));
        cacheConfiguration.setQueryEntities(queryEntities);

        return new TransformOutput.Builder()
                .setPCollection(pCollection)
                .setFields(fields.toArray(new String[0]))
                .setSchema(newSchema)
                .setCacheComponents(out.getCacheComponents())
                .setQueryEntities(queryEntities)
                .setCacheConfiguration(cacheConfiguration)
                .setCacheDataTypes(out.getCacheDataTypes())
                .build();
    }

    /**
     * Creates copy field processor, copies field for beam rows,
     * expects provided beam schema contains new field schema
     */
    private static class CopyFieldProcessor {

        public static RowElementsProcessor create(org.apache.beam.sdk.schemas.Schema beamSchema, String fieldToCopy, String newFieldName) {
            RowFunction copyFunction = (RowFunction & Serializable) (Row row) -> {
                Row.Builder rowBuilder = Row.withSchema(beamSchema);
                Row.FieldValueBuilder fieldValueBuilder = null;
                for (org.apache.beam.sdk.schemas.Schema.Field field : beamSchema.getFields()) {
                    Object val = field.getName().equals(newFieldName)
                            ? row.getValue(fieldToCopy)
                            : row.getValue(field.getName());
                    if (fieldValueBuilder == null) {
                        fieldValueBuilder = rowBuilder.withFieldValue(field.getName(), val);
                    }
                    fieldValueBuilder.withFieldValue(field.getName(), val);
                }
                if (fieldValueBuilder == null) {
                    throw new IllegalStateException("No fields found when executing copy field action");
                }
                return fieldValueBuilder.build();
            };

            return new RowElementsProcessor(copyFunction);
        }
    }

    public static class Builder {
        private TransformAction<TransformOutput> action;
        private String fieldToCopy;
        private String newFieldName;

        public Builder action(TransformAction<TransformOutput> action) {
            this.action = action;
            return this;
        }

        public Builder copyField(String fieldToCopy, String newFieldName) {
            this.fieldToCopy = fieldToCopy;
            this.newFieldName = newFieldName;
            return this;
        }

        private void validate() {
            Objects.requireNonNull(action, "parent action is null");
            Objects.requireNonNull(newFieldName, "new field name is not set");
            Objects.requireNonNull(fieldToCopy, "field to copy is not set");
        }

        public CopyFieldAction build() {
            validate();
            return new CopyFieldAction(this);
        }
    }
}
