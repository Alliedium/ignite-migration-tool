package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.Utils;

/**
 * Converter used for getting a String representation of complex Apache Ignite entities, which need to remain having ignite-specific data.
 * Resulting String contains an Apache Ignite object converted to an XML.
 * Reverse conversion is also available.
 */
public class IgniteObjectStringConverter implements IIgniteDTOConverter<String, Object> {

    public String convertFromEntity(Object entity) {
        return Utils.serializeObjectToXML(entity);
    }

    // todo: refactor this method, it leads to type casts
    public Object convertFromDto(String dto) {
        return Utils.deserializeFromXML(dto);
    }

}
