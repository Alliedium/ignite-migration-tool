package com.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.serializer.CacheAvroFilesLocator;
import org.apache.avro.Schema;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
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

    static boolean checkCacheFilesExist(CacheComponent cacheComponent) {
        CacheAvroFilesLocator filesLocator = cacheComponent.getFilesLocator();
        List<Path> pathsWhichDoNotExist = Stream.of(filesLocator.cacheDataPath(), filesLocator.cacheConfigurationPath(),
                        filesLocator.cacheConfigurationSchemaPath(), filesLocator.cacheDataSchemaPath())
                .filter(path -> !Files.exists(path))
                .collect(Collectors.toList());

        if (pathsWhichDoNotExist.size() > 0) {
            System.out.println("The following cache files where not found " + pathsWhichDoNotExist);
        }

        return pathsWhichDoNotExist.size() == 0;
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

    static void patchCachesWhichEndWith(PatchContext context, String cachesEndWith, Consumer<String> patch) {
        List<String> cachePaths = context.selectCachePathsWhichEndWith(cachesEndWith);
        AtomicInteger counter = new AtomicInteger();
        cachePaths.forEach(cachePath -> {
            boolean allFilesExist = Util.checkCacheFilesExist(context.getCacheComponent(cachePath));
            if (!allFilesExist) {
                System.out.println(String.format(
                        "Cache %s was skipped cause not all files exist for this cache, that's why it is invalid", cachePath));
                counter.incrementAndGet();
                return;
            }

            patch.accept(cachePath);
        });
        System.out.println("=========== MISSED CACHES COUNTER " + counter.get());
    }
}
