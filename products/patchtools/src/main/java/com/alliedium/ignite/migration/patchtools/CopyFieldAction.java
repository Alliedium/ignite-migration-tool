package com.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.beam.sdk.schemas.transforms.AddFields;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.*;
import java.util.stream.Collectors;

public class CopyFieldAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> selectAction;
    private String fieldToCopy;
    private String newFieldName;
    private RowElementsProcessor elementsProcessor;

    public CopyFieldAction(TransformAction<TransformOutput> selectAction) {
        this.selectAction = selectAction;
    }

    public CopyFieldAction copyField(String fieldToCopy, String newFieldName) {
        this.fieldToCopy = fieldToCopy;
        this.newFieldName = newFieldName;
        this.elementsProcessor = new RowElementsProcessor(row ->
                Row.fromRow(row)
                        .withFieldValue(newFieldName, row.getValue(fieldToCopy))
                        .build());
        return this;
    }

    public TransformOutput execute() {
        TransformOutput out = selectAction.execute();
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
                .apply(AddFields.<Row>create()
                        .field(newFieldName, beamSchema.getField(newFieldName).getType()));
        pCollection = pCollection
                .apply(ParDo.of(elementsProcessor)).setCoder(pCollection.getCoder());

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
                .build();
    }
}
