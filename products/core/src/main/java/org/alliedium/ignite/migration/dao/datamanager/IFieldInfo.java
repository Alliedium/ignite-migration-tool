package org.alliedium.ignite.migration.dao.datamanager;

import org.alliedium.ignite.migration.dao.converters.IIgniteBinaryDataConverter;

import java.util.Map;

/**
 * Provides an access to meta-data of separate Apache Ignite cache field (field name, value type class name,
 * converter needed to get a common Java representation when the field value is of Apache Ignite specific class).
 * Returning meta-data is required for Apache Ignite cache field serialization to avro.
 */
public interface IFieldInfo {

    String getName();

    String getTypeInfo();

    Map<String, IFieldInfo> getNested();

    IIgniteBinaryDataConverter getFieldDataConverter();

}
