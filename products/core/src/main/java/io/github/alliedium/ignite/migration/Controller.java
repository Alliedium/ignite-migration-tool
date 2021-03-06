package io.github.alliedium.ignite.migration;

import io.github.alliedium.ignite.migration.dao.*;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IIgniteDAO;
import io.github.alliedium.ignite.migration.dao.dataaccessor.IgniteDAO;
import io.github.alliedium.ignite.migration.dto.ICacheData;
import io.github.alliedium.ignite.migration.dto.ICacheMetaData;
import io.github.alliedium.ignite.migration.properties.PropertiesResolver;
import io.github.alliedium.ignite.migration.serializer.AvroDeserializer;
import io.github.alliedium.ignite.migration.serializer.AvroSerializer;
import io.github.alliedium.ignite.migration.serializer.IAvroDeserializer;
import io.github.alliedium.ignite.migration.serializer.ISerializer;

import java.nio.file.Path;
import java.util.Map;

import org.apache.ignite.Ignite;

/**
 * Controller is a unit responsible for CLI general commands processing: data serialization (from Apache Ignite to avro) and deserialization (from avro to ignite).
 * It requires an Apache Ignite connection to be passed in on the initialization stage.
 * Controller is used to be a linker between two general application modules: DAO and Serializer.
 *
 * @see ISerializer
 * @see IIgniteReader
 * @see IgniteWritersFactory
 */
public class Controller {

    private final IIgniteReader igniteScanner;
    private final IgniteWritersFactory igniteWriter;
    private final IIgniteDAO igniteDAO;
    private final DispatcherFactory dispatcherFactory;
    private final Ignite ignite;
    private final PropertiesResolver propertiesResolver;

    public Controller(Ignite ignite, IgniteAtomicLongNamesProvider atomicNamesProvider) {
        this(ignite, atomicNamesProvider, PropertiesResolver.loadProperties());
    }

    public Controller(Ignite ignite, IgniteAtomicLongNamesProvider atomicNamesProvider, PropertiesResolver propertiesResolver) {
        igniteScanner = new IgniteScanner(atomicNamesProvider, ignite);
        igniteWriter = new IgniteWritersFactoryImpl(ignite);
        igniteDAO = new IgniteDAO(ignite);
        this.dispatcherFactory = new DispatcherFactory(propertiesResolver);
        this.ignite = ignite;
        this.propertiesResolver = propertiesResolver;
    }

    /**
     * Represents serialize operation for Apache Ignite data
     * @param rootSerializedDataPath - path to which Apache Ignite cluster data will be serialized
     */
    public void serializeDataToAvro(Path rootSerializedDataPath) {
        ISerializer avroSerializer = new AvroSerializer(rootSerializedDataPath);
        Dispatcher<ICacheMetaData> cacheMetaDataDispatcher = dispatcherFactory.newDispatcher();
        cacheMetaDataDispatcher.subscribe(avroSerializer.getCacheMetaDataSerializer());

        Dispatcher<ICacheData> cacheDataDispatcher = dispatcherFactory.newDispatcher();
        cacheDataDispatcher.subscribe(avroSerializer.getCacheDataSerializer());

        Dispatcher<Map.Entry<String, Long>> atomicsLongDispatcher = dispatcherFactory.newDispatcher();
        atomicsLongDispatcher.subscribe(avroSerializer.getAtomicLongsConsumer());

        Runnable scanner = () -> igniteScanner.read(igniteDAO, cacheMetaDataDispatcher, cacheDataDispatcher, atomicsLongDispatcher);

        TasksExecutor executor = TasksExecutor.execute(scanner, cacheDataDispatcher, cacheMetaDataDispatcher, atomicsLongDispatcher);
        if (propertiesResolver.closeIgniteInstanceAfterRun()) {
            executor.registerResourcesToCloseOnShutdown(ignite);
        }
        executor.waitForCompletion();
    }

    /**
     * Represents a deserialize operation of avro file right into Apache Ignite.
     * In case a cache exists both in Apache Ignite and in provided avro files,
     * the Deserialize operation will deserialize and override values by keys, no other objects would be touched.
     * For example if a cache contains the following data:
     *      [first : firstObj, second : secondObj]
     * And serialized data contains the following data:
     *      [first : thirdObj]
     * The result of deserialize operation will be the following:
     *      [first : thirdObj, second : secondObj]
     *
     * @param avroFilesPath - path to avro files which contain serialized data.
     */
    public void deserializeDataFromAvro(Path avroFilesPath) {
        IAvroDeserializer avroDeserializer = new AvroDeserializer(avroFilesPath);

        Dispatcher<ICacheMetaData> cacheMetaDataDispatcher = dispatcherFactory.newDispatcher();
        cacheMetaDataDispatcher.subscribe(igniteWriter.getIgniteCacheMetaDataWriter());

        Dispatcher<ICacheData> cacheDataDispatcher = dispatcherFactory.newDispatcher();
        IgniteCacheDataWriter igniteCacheDataWriter = igniteWriter.getIgniteCacheDataWriter();
        cacheDataDispatcher.subscribe(igniteCacheDataWriter);
        cacheMetaDataDispatcher.subscribe(igniteCacheDataWriter.getMetaDataConsumer());

        Dispatcher<Map.Entry<String, Long>> atomicsLongDispatcher = dispatcherFactory.newDispatcher();
        atomicsLongDispatcher.subscribe(igniteWriter.getIgniteAtomicsLongDataWriter());

        Runnable deserializer = () -> {
            avroDeserializer.deserializeCaches(cacheMetaDataDispatcher, cacheDataDispatcher);
            avroDeserializer.deserializeAtomicsLong(atomicsLongDispatcher);
        };

        TasksExecutor executor = TasksExecutor.execute(deserializer, cacheDataDispatcher, cacheMetaDataDispatcher, atomicsLongDispatcher);
        if (propertiesResolver.closeIgniteInstanceAfterRun()) {
            executor.registerResourcesToCloseOnShutdown(ignite);
        }
        executor.waitForCompletion();
    }
}
