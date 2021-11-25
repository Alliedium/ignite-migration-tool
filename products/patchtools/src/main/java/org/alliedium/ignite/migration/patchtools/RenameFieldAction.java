package org.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.beam.sdk.schemas.transforms.RenameFields;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.*;
import java.util.stream.Collectors;

public class RenameFieldAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> action;
    private final String oldFieldName;
    private final String newFieldName;

    private RenameFieldAction(Builder builder) {
        action = builder.action;
        oldFieldName = builder.oldFieldName;
        newFieldName = builder.newFieldName;
    }

    @Override
    public TransformOutput execute() {
        TransformOutput out = action.execute();
        Set<String> fields = Arrays.stream(out.getFields()).collect(Collectors.toSet());
        fields.remove(oldFieldName);
        fields.add(newFieldName);

        Schema schema = out.getSchema();
        List<Schema.Field> schemaFields = new ArrayList<>(schema.getFields());

        Schema.Field oldSchemaField = Util.searchFieldByName(out, oldFieldName);
        Schema.Field newSchemaField = new Schema.Field(newFieldName, oldSchemaField.schema(),
                oldSchemaField.doc(), oldSchemaField.defaultVal(), oldSchemaField.order());

        Set<Schema.Field> newSchemaFields = schemaFields.stream().filter(field -> !oldFieldName.equals(field.name()))
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
                .collect(Collectors.toSet());

        newSchemaFields.add(newSchemaField);

        Schema newSchema = Schema.createRecord(schema.getName(), schema.getDoc(),
                schema.getNamespace(), false, new ArrayList<>(newSchemaFields));

        PCollection<Row> pCollection = out.getPCollection()
                .apply(RenameFields.<Row>create().rename(oldFieldName, newFieldName));

        CacheConfiguration<Object, BinaryObject> cacheConfiguration = out.getCacheConfiguration();
        Collection<QueryEntity> queryEntities = out.getQueryEntities();
        queryEntities.forEach(queryEntity -> QueryEntityUtils.renameField(queryEntity, oldFieldName, newFieldName));
        cacheConfiguration.setQueryEntities(queryEntities);

        return new TransformOutput.Builder()
                .setPCollection(pCollection)
                .setFields(fields.toArray(new String[0]))
                .setSchema(newSchema)
                .setCacheComponents(out.getCacheComponents())
                .setCacheConfiguration(cacheConfiguration)
                .setQueryEntities(queryEntities)
                .build();
    }

    public static class Builder {
        private TransformAction<TransformOutput> action;
        private String oldFieldName;
        private String newFieldName;

        public Builder action(TransformAction<TransformOutput> action) {
            this.action = action;
            return this;
        }

        public Builder renameField(String oldFieldName, String newFieldName) {
            this.newFieldName = newFieldName;
            this.oldFieldName = oldFieldName;
            return this;
        }

        private void validate() {
            Objects.requireNonNull(action, "parent action is null");
            Objects.requireNonNull(newFieldName, "new field name not set");
            Objects.requireNonNull(oldFieldName, "old field name not set");
        }

        public RenameFieldAction build() {
            validate();
            return new RenameFieldAction(this);
        }
    }
}
