package org.alliedium.ignite.migration.dao.converters;

import org.alliedium.ignite.migration.Utils;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;

import java.util.Collection;

/**
 * Converter used for getting a String representation of complex Apache Ignite entities, which need to remain having ignite-specific data.
 * Resulting String contains an Apache Ignite object converted to an XML.
 * Reverse conversion is also available.
 */
public class IgniteObjectStringConverter<T> implements IIgniteDTOConverter<String, T> {

    public static final IIgniteDTOConverter<String, Object> GENERIC_CONVERTER = new IgniteObjectStringConverter<>();
    public static final IIgniteDTOConverter<String, Collection<QueryEntity>> QUERY_ENTITY_CONVERTER = new IgniteObjectStringConverter<>();
    public static final IIgniteDTOConverter<String, CacheConfiguration<Object, BinaryObject>> CACHE_CONFIG_CONVERTER = new IgniteObjectStringConverter<>();

    public String convertFromEntity(T entity) {
        return Utils.serializeObjectToXML(entity);
    }

    public T convertFromDto(String dto) {
        return Utils.deserializeFromXML(dto);
    }

}
