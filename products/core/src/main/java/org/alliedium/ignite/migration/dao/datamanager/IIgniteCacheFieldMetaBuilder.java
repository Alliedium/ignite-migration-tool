package org.alliedium.ignite.migration.dao.datamanager;

import java.util.Map;

/**
 * Provides an access to a list of Apache Ignite cache fields' meta-data containers. Each container from
 * the returning list corresponds to a separate Apache Ignite cache field.
 */
public interface IIgniteCacheFieldMetaBuilder {

    Map<String, IIgniteCacheFieldMeta> getFieldsMetaData();

}
