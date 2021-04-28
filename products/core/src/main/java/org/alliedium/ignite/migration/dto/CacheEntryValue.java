package org.alliedium.ignite.migration.dto;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Contains and returns data fields from cache entry.
 * Particular cache entry field can be queried by field name (list of which is also
 * available for querying). Returning value is {@link ICacheEntryValueField}.
 */
public class CacheEntryValue implements ICacheEntryValue {

    private final Map<String, ICacheEntryValueField> valueFieldNameToObjectMap;

    public CacheEntryValue(List<ICacheEntryValueField> cacheEntryValueDTOFieldList) {
        this.valueFieldNameToObjectMap = cacheEntryValueDTOFieldList.stream()
            .collect(Collectors.toMap(ICacheEntryValueField::getName, ICacheEntryValueDTOField -> ICacheEntryValueDTOField, (u, v) -> {
                throw new IllegalStateException(String.format("Duplicate fieldName %s", u));
            }, LinkedHashMap::new));
    }

    public List<String> getFieldNamesList() {
        return new ArrayList<>(valueFieldNameToObjectMap.keySet());
    }

    public List<ICacheEntryValueField> getFields() {
        return new ArrayList<>(valueFieldNameToObjectMap.values());
    }

    public ICacheEntryValueField getField(String fieldName) {
        return valueFieldNameToObjectMap.get(fieldName);
    }
}
