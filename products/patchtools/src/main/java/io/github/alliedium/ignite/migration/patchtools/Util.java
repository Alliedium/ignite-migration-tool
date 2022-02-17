package io.github.alliedium.ignite.migration.patchtools;

import org.apache.avro.Schema;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Util {
    public static Schema.Field searchFieldByName(DataComponentsProvider dataProvider, String fieldName) {
        Schema schema = dataProvider.getSchema();
        List<Schema.Field> schemaFields = new ArrayList<>(schema.getFields());
        Optional<Schema.Field> oldSchemaFieldOptional = schemaFields.stream()
                .filter(field -> field.name().equals(fieldName)).findFirst();
        if (!oldSchemaFieldOptional.isPresent()) {
            throw new IllegalArgumentException(String.format(
                    "no field found with name %s in cache schema %s", fieldName, dataProvider.getFirstCacheComponent().getCachePath()));
        }

        return oldSchemaFieldOptional.get();
    }

    static Schema filterSchemaFields(Schema schema, String... fields) {
        List<Schema.Field> schemaFields = new ArrayList<>(schema.getFields());
        Set<String> fieldSet = Stream.of(fields).collect(Collectors.toSet());

        Stream<Schema.Field> stream = schemaFields.stream();
        if (fieldSet.size() > 0) {
            stream = stream.filter(field -> fieldSet.contains(field.name()));
        }

        Set<Schema.Field> pipelineSchemaFields = stream
                .map(field -> new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultVal(), field.order()))
                .collect(Collectors.toSet());

        return Schema.createRecord(
                schema.getName(),
                schema.getDoc(),
                schema.getNamespace(), false,
                new ArrayList<>(pipelineSchemaFields));
    }

    static QueryEntity filterQueryEntityFields(QueryEntity queryEntity, String... fields) {
        QueryEntity newQueryEntity = new QueryEntity(queryEntity);
        LinkedHashMap<String, String> newQueryEntityFields = newQueryEntity.getFields();
        LinkedHashMap<String, String> queryEntityFields = queryEntity.getFields();
        Set<String> fieldSet = Stream.of(fields).collect(Collectors.toSet());
        queryEntityFields.forEach((name, type) -> {
            if (!containsIgnoreCase(fieldSet, name)) {
                newQueryEntityFields.remove(name);
                newQueryEntity.setFields(newQueryEntityFields);
                Map<String, String> aliases = newQueryEntity.getAliases();
                aliases.remove(name);
                newQueryEntity.setAliases(aliases);
            }
        });

        return newQueryEntity;
    }

    static QueryEntity filterQueryEntityIndexes(QueryEntity queryEntity, String... fields) {
        QueryEntity newQueryEntity = new QueryEntity(queryEntity);
        Collection<QueryIndex> newQueryEntityIndexes = newQueryEntity.getIndexes();
        Collection<QueryIndex> queryEntityIndexes = queryEntity.getIndexes();
        Set<String> fieldSet = Stream.of(fields).collect(Collectors.toSet());
        queryEntityIndexes.forEach((queryIndex) -> {
            if (!containsIgnoreCase(fieldSet, queryIndex.getName())) {
                newQueryEntityIndexes.remove(queryIndex);
            }
        });

        newQueryEntity.setIndexes(newQueryEntityIndexes);

        return newQueryEntity;
    }

    private static boolean containsIgnoreCase(Set<String> set, String name) {
        return set.stream().anyMatch(i -> i.equalsIgnoreCase(name));
    }
}
