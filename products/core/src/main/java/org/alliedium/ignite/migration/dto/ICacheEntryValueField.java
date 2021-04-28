package org.alliedium.ignite.migration.dto;

/**
 * Provides an access to meta-data (field name, field value type class name) and value of particular cache entry field.
 * Can be accessed from parent entity {@link ICacheEntryValue}, which is a container for all cache entry fields.
 *
 * @see ICacheEntryValueField#getFieldValue()
 */
public interface ICacheEntryValueField {

    String getName();

    String getTypeClassName();

    ICacheEntryValueFieldValue getFieldValue();

}
