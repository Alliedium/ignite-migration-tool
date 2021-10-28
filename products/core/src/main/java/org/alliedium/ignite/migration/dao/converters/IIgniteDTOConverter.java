package org.alliedium.ignite.migration.dao.converters;

/**
 * Provides a functionality of converting an ignite-specific entity to correspondent DTO representation.
 * Reverse conversion is also available.
 *
 * @param <T>
 * @param <U>
 */
public interface IIgniteDTOConverter<T, U> {

    T convertFromEntity(U entity);

    U convertFromDTO(T dto);

}
