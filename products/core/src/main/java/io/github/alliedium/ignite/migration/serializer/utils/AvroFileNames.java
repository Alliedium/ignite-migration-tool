package io.github.alliedium.ignite.migration.serializer.utils;

/**
 * Unit contains names for avro files.
 * Files location directory needs to be provided on the initialization.
 * Returning filename considers location directory and filetype
 * (cache data / avro schema for cache data / cache configurations / avro schema for cache configurations).
 */
public interface AvroFileNames {

    /**
     * represents location of atomics avro schema structure files
     */
    String ATOMIC_STRUCTURE_FILE_NAME = "/atomic_structure" + AvroFileExtensions.AVSC;
    /**
     * represents location of atomics avro schema data files
     */
    String ATOMIC_DATA_FILE_NAME = "/atomic_data" + AvroFileExtensions.AVRO;

    /**
     * Responsible for producing the name for avro file where cache data is going to be stored.
     * ".avro" extension is used for files containing avro data.
     */
    String CACHE_DATA_FILENAME = "/cache" + AvroFileExtensions.AVRO;

    /**
     * Responsible for producing the name for file containing avro schema, which describes avro file where cache data is stored.
     * ".avsc" extension is used for files containing avro data.
     *
     * @see <a href="http://avro.apache.org/docs/current/index.html#schemas">Avro schema Introduction</a>
     */
    String SCHEMA_FOR_CACHE_DATA_FILENAME = "/schema_cache" + AvroFileExtensions.AVSC;

    /**
     * Responsible for producing the name for avro file where cache configurations are going to be stored.
     * ".avro" extension is used for files containing avro data.
     */
    String CACHE_CONFIGURATION_FILENAME = "/configuration" + AvroFileExtensions.AVRO;

    /**
     * Responsible for producing the name for file containing avro schema, which describes avro file where cache configurations are stored.
     * ".avsc" extension is used for files containing avro data.
     *
     * @see <a href="http://avro.apache.org/docs/current/index.html#schemas">Avro schema Introduction</a>
     */
    String SCHEMA_FOR_CACHE_CONFIGURATION_FILENAME = "/schema_configuration" + AvroFileExtensions.AVSC;
}
