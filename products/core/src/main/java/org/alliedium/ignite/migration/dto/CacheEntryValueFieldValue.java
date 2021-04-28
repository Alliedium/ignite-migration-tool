package org.alliedium.ignite.migration.dto;

import java.util.Optional;

/**
 * Contains and returns value of particular cache entry field.
 * Ignite cache can have 'null' values stored in any of the fields. Hence current unit can possibly be initialized with 'null' passed as argument.
 * Returning cache entry field value is covered with {@link java.util.Optional} in order to avoid 'null' values in output.
 */
public class CacheEntryValueFieldValue implements ICacheEntryValueFieldValue {

    private final Object value;

    public CacheEntryValueFieldValue(Object value) {
        this.value = value;
    }

    public static CacheEntryValueFieldValue noValue() {
        return new CacheEntryValueFieldValue(null);
    }

    public Optional<Object> getValue() {
        return Optional.ofNullable(value);
    }

}
