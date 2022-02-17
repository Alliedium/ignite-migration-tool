package io.github.alliedium.ignite.migration.serializer.converters.datatypes;

/**
 * Converter required for avro-serialization of the data types, which are not supported.
 * DTO to avro and backward avro to DTO conversion is considered.
 */
public interface IAvroDerivedTypeConverter {

    Object convertForAvro(Object fieldData);

    Object convertFromAvro(Object fieldData);

}
