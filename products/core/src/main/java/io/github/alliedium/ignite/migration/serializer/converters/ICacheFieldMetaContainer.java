package io.github.alliedium.ignite.migration.serializer.converters;

import io.github.alliedium.ignite.migration.dto.ICacheEntryValueField;

/**
 * Provides a functionality of storing and returning the {@link ICacheFieldMeta}.
 * Meta-data for each separate {@link ICacheEntryValueField} can be
 * requested from current container by field name ({@link ICacheEntryValueField#getName()}).
 * Needs to be created in terms of serializing DTO to avro.
 */
public interface ICacheFieldMetaContainer {

    ICacheFieldMeta getFieldTypeMeta(String fieldName);

}
