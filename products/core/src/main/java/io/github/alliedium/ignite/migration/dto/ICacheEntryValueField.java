package io.github.alliedium.ignite.migration.dto;

import java.util.List;
import java.util.Optional;

/**
 * Provides an access to meta-data (field name, field value type class name) and value of particular cache entry field.
 * Can be accessed from parent entity {@link ICacheEntryValue}, which is a container for all cache entry fields.
 *
 * @see ICacheEntryValueField#getFieldValue()
 */
public interface ICacheEntryValueField {

    String getName();

    String getTypeClassName();

    Optional<Object> getFieldValue();

    List<ICacheEntryValueField> getNested();

    boolean hasNested();
}
