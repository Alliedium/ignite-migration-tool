package org.alliedium.ignite.migration.dto;

/**
 * Contains and returns meta-data and value of particular cache entry field.
 * Returning cache entry field value is represented as child {@link ICacheEntryValueFieldValue}.
 */
public class CacheEntryValueField implements ICacheEntryValueField {

    private final String name;
    private final String typeClassName;
    private final ICacheEntryValueFieldValue fieldValue;

    public CacheEntryValueField(String name, String typeClassName, ICacheEntryValueFieldValue fieldValue) {
        this.name = name;
        this.typeClassName = typeClassName;
        this.fieldValue = fieldValue;
    }

    public String getName() {
        return this.name;
    }

    public String getTypeClassName() {
        return this.typeClassName;
    }

    public ICacheEntryValueFieldValue getFieldValue() {
        return this.fieldValue;
    }

}
