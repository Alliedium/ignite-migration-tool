package org.alliedium.ignite.migration.patchtools;

import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.QueryIndex;

import java.util.*;

public class QueryEntityUtils {
    public static void copyField(QueryEntity queryEntity, String fieldToCopy, String newFieldName) {
        LinkedHashMap<String, String> fields = queryEntity.getFields();
        addField(queryEntity, newFieldName.toUpperCase(), fields.get(fieldToCopy.toUpperCase()));
    }

    public static void renameField(QueryEntity queryEntity, String oldFieldName, String newFieldName) {
        oldFieldName = oldFieldName.toUpperCase();
        newFieldName = newFieldName.toUpperCase();
        LinkedHashMap<String, String> fields = queryEntity.getFields();
        String className = fields.remove(oldFieldName);
        fields.put(newFieldName, className);
        queryEntity.setFields(fields);
        Map<String, String> aliases = queryEntity.getAliases();
        aliases.remove(oldFieldName);
        aliases.put(newFieldName, newFieldName);
        queryEntity.setAliases(aliases);
    }

    private static void addField(QueryEntity queryEntity, String fieldName, String className) {
        LinkedHashMap<String, String> fields = queryEntity.getFields();
        fields.put(fieldName, className);
        queryEntity.setFields(fields);
        Map<String, String> aliases = queryEntity.getAliases();
        aliases.put(fieldName, fieldName.toUpperCase());
        queryEntity.setAliases(aliases);
    }

    public static QueryEntity joinQueryEntities(QueryEntity... queryEntities) {
        if (queryEntities.length == 0) {
            throw new IllegalArgumentException("Cannot execute join of query entities, no query entities provided");
        }
        if (queryEntities.length == 1) {
            return queryEntities[0];
        }

        QueryEntity resultQueryEntity = new QueryEntity(queryEntities[0]);
        LinkedHashMap<String, String> fieldMap = new LinkedHashMap<>();
        Map<String, String> aliases = new HashMap<>();
        List<QueryIndex> queryIndexes = new ArrayList<>();
        for (QueryEntity nextQueryEntities : queryEntities) {
            fieldMap.putAll(nextQueryEntities.getFields());
            aliases.putAll(nextQueryEntities.getAliases());
            queryIndexes.addAll(nextQueryEntities.getIndexes());
        }
        resultQueryEntity.setFields(fieldMap);
        resultQueryEntity.setAliases(aliases);
        resultQueryEntity.setIndexes(queryIndexes);
        return resultQueryEntity;
    }
}
