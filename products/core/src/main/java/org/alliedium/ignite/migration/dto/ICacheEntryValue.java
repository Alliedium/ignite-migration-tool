package org.alliedium.ignite.migration.dto;

import java.util.List;

/**
 * Provides an access to ignite cache entry data fields.
 * List of cache field names and particular cache field (represented as {@link ICacheEntryValueField} can be queried.
 * ICacheEntryValue alongside with {@link ICacheEntryKey} are being a separate 'key:value' unit of cache data.
 */
public interface ICacheEntryValue {

    List<String> getFieldNamesList();

    ICacheEntryValueField getField(String fieldName);

    List<ICacheEntryValueField> getFields();
}
