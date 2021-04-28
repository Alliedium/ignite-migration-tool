package org.alliedium.ignite.migration.dao.datamanager;

import org.alliedium.ignite.migration.dao.converters.IIgniteBinaryDataConverter;

/**
 * Provides an access to meta-data of separate ignite cache field (field name, value type class name,
 * converter needed to get a common Java representation when the field value is of Ignite specific class).
 * Returning meta-data is required for ignite cache field serialization to avro.
 */
public interface IIgniteCacheFieldMeta {

    String getName();

    String getTypeInfo();

    IIgniteBinaryDataConverter getFieldDataConverter();

}
