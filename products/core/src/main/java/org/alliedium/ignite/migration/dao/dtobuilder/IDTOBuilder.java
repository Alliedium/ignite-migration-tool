package org.alliedium.ignite.migration.dao.dtobuilder;

/**
 * Provides a functionality of building correspondent DTO objects from ignite data.
 *
 * @param <T>
 */
public interface IDTOBuilder<T> {

    T build();

}
