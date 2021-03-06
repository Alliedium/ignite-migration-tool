package io.github.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.beam.sdk.schemas.transforms.CoGroup;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.beam.sdk.schemas.transforms.Select.fieldNames;

public class JoinAction implements TransformAction<TransformOutput> {
    private final TransformAction<TransformOutput> first;
    private final TransformAction<TransformOutput> second;
    private final String on;

    private JoinAction(Builder builder) {
        this.first = builder.first;
        this.second = builder.second;
        this.on = builder.on;
    }

    public TransformOutput execute() {
        TransformOutput output1 = first.execute();
        TransformOutput output2 = second.execute();

        String tableName1 = "input1";
        String tableName2 = "input2";

        PCollection<Row> joined = PCollectionTuple.of(tableName1, output1.getPCollection(), tableName2, output2.getPCollection())
                .apply(CoGroup.join(tableName1, CoGroup.By.fieldNames(on))//.withOptionalParticipation())
                        .join(tableName2, CoGroup.By.fieldNames(on))
                        .crossProductJoin());

        Schema schema1 = output1.getSchema();
        Schema schema2 = output2.getSchema();

        Set<Schema.Field> fields = new HashSet<>();
        fields.addAll(schema1.getFields());
        fields.addAll(schema2.getFields());

        Set<String> joinedFieldNames = new HashSet<>();
        joinedFieldNames.addAll(Arrays.asList(output1.getFields()));
        joinedFieldNames.addAll(Arrays.asList(output2.getFields()));

        Set<Schema.Field> pipelineSchemaFields = fields.stream().filter(field -> joinedFieldNames.contains(field.name()))
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
                .collect(Collectors.toSet());

        Schema joinedSchema = Schema.createRecord(
                schema1.getName() + "_joined_" + schema2.getName(),
                "new schema for pipeline",
                schema1.getNamespace(), false,
                new ArrayList<>(pipelineSchemaFields));


        Set<String> resultFields = new HashSet<>();
        resultFields.addAll(Stream.of(output1.getFields())
                .map(field -> tableName1 + "." + field).collect(Collectors.toSet()));
        resultFields.addAll(Stream.of(output2.getFields())
                .filter(field -> !on.equals(field))
                .map(field -> tableName2 + "." + field)
                .collect(Collectors.toSet()));

        joined = joined.apply(
                fieldNames(resultFields.toArray(new String[0])));

        CacheConfiguration<Object, BinaryObject> cacheConfiguration = output1.getCacheConfiguration();
        Collection<QueryEntity> resultQueryEntities = new ArrayList<>();
        List<QueryEntity> queryEntities1 = new ArrayList<>(output1.getQueryEntities());
        List<QueryEntity> queryEntities2 = new ArrayList<>(output2.getQueryEntities());
        int length = queryEntities1.size() + queryEntities2.size();

        for (int i = 0, j = 0; i + j < length;) {
            QueryEntity queryEntity1 = null;
            if (queryEntities1.size() > i) {
                queryEntity1 = queryEntities1.get(i);
                i++;
            }
            QueryEntity queryEntity2 = null;
            if (queryEntities2.size() > j) {
                queryEntity2 = queryEntities2.get(j);
                j++;
            }

            QueryEntity[] queryEntities = Stream.of(queryEntity1, queryEntity2)
                    .filter(Objects::nonNull)
                    .toArray(QueryEntity[]::new);
            resultQueryEntities.add(QueryEntityUtils.joinQueryEntities(queryEntities));
        }

        cacheConfiguration.setQueryEntities(resultQueryEntities);

        return new TransformOutput.Builder()
                .setPCollection(joined)
                .setFields(joinedFieldNames.toArray(new String[0]))
                .setSchema(joinedSchema)
                .setCacheComponents(
                        Stream.of(output1.getCacheComponents(), output2.getCacheComponents())
                                .flatMap(Stream::of).toArray(CacheComponent[]::new))
                .setQueryEntities(resultQueryEntities)
                .setCacheConfiguration(cacheConfiguration)
                .setCacheDataTypes(output1.getCacheDataTypes())
                .build();
    }

    public static class Builder {
        private TransformAction<TransformOutput> first;
        private TransformAction<TransformOutput> second;
        private String on;

        public Builder join(TransformAction<TransformOutput> first,
                               TransformAction<TransformOutput> second) {
            this.first = first;
            this.second = second;
            return this;
        }

        public Builder on(String field) {
            this.on = field;
            return this;
        }

        public void validate() {
            Objects.requireNonNull(first, "[Join action] first action should not be null");
            Objects.requireNonNull(second, "[Join action] second action should not be null");
            Objects.requireNonNull(on, "[Join action] join field was not provided");
        }

        public JoinAction build() {
            return new JoinAction(this);
        }
    }
}
