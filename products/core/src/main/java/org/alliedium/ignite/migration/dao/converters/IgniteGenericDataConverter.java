package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.dao.datamanager.IgniteCacheFieldMeta;
import org.alliedium.ignite.migration.dao.datamanager.IgniteCacheFieldMetaBuilder;

/**
 * Non-functional converter.
 * Since {@link IgniteCacheFieldMeta} container needs any converter to be assigned to each of Apache Ignite cache fields,
 * current converter is being used for fields, whose values don't need to be converted (== value type is not ignite-specific).
 *
 * @see IgniteCacheFieldMetaBuilder
 */
public class IgniteGenericDataConverter implements IIgniteBinaryDataConverter {

    public Object convert(Object fieldData) {
        return fieldData;
    }

}
