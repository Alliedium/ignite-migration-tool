package org.alliedium.ignite.migration.dto;

import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Contains and returns meta-data and value of particular cache entry field.
 * Returning cache entry field value is represented as Object.
 * Has nested fields of the same type in this case it represents complex object with nested fields/objects.
 */
public class CacheEntryValueField implements ICacheEntryValueField {

    private final String name;
    private final String typeClassName;

    /**
     * Provides access to cache entry field value.
     * Can be accessed from parent entity
     */
    @Nullable
    private final Object value;

    private final List<ICacheEntryValueField> nested;

    private CacheEntryValueField(Builder builder) {
        name = builder.name;
        typeClassName = builder.typeClassName;
        value = builder.value;
        nested = builder.nested;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    public String getTypeClassName() {
        return this.typeClassName;
    }

    @Override
    public Optional<Object> getFieldValue() {
        return Optional.ofNullable(value);
    }

    @Override
    public List<ICacheEntryValueField> getNested() {
        return new ArrayList<>(nested);
    }

    @Override
    public boolean hasNested() {
        return !nested.isEmpty();
    }

    @Override
    public String toString() {
        return "CacheEntryValueField{" +
                "name='" + name + '\'' +
                ", typeClassName='" + typeClassName + '\'' +
                ", value=" + value +
                ", nested=" + nested +
                '}';
    }

    public static class Builder {
        private String name;
        private String typeClassName;
        private Object value;
        private List<ICacheEntryValueField> nested;

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public Builder setTypeClassName(String typeClassName) {
            this.typeClassName = typeClassName;
            return this;
        }

        public Builder setValue(Object value) {
            this.value = value;
            return this;
        }

        public Builder setNested(List<ICacheEntryValueField> nested) {
            this.nested = new ArrayList<>(nested);
            return this;
        }

        public CacheEntryValueField build() {
            nested = nested == null ? Collections.emptyList() : nested;
            return new CacheEntryValueField(this);
        }
    }
}
