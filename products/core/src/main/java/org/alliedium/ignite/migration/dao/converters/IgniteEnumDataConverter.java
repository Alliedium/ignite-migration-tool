package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.dto.CacheEntryValueFieldValue;
import org.apache.ignite.internal.binary.BinaryEnumObjectImpl;

/**
 * Responsible for conversion from {@link BinaryEnumObjectImpl} to String.
 * Used in order to get rid of ignite-specific types when
 * storing an Apache Ignite cache field value in DTO as {@link CacheEntryValueFieldValue}
 */
public class IgniteEnumDataConverter implements
    IIgniteBinaryDataConverter { //TODO: either enhance current converter to not to use String for output, or get rid of current converter since BinaryEnumObjectImpl is not used in project anymore

    public String convert(Object fieldData) {
        assert fieldData instanceof BinaryEnumObjectImpl;
        return ((BinaryEnumObjectImpl) fieldData).enumName();
    }

}
