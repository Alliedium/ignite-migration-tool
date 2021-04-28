package org.alliedium.ignite.migration.dao.converters;

/**
 * Provides a functionality of converting an ignite-specific data value to common Java type.
 */
@FunctionalInterface
public interface IIgniteBinaryDataConverter {

    Object convert(Object fieldData);

}
