package io.github.alliedium.ignite.migration.dao.dtobuilder;

/**
 * Provides a functionality of building correspondent DTO objects from Apache Ignite data.
 *
 * @param <T>
 */
public interface IDTOBuilder<T> {

    T build();

}
