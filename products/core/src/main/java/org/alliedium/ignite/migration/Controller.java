package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.dao.IIgniteReader;
import org.alliedium.ignite.migration.dao.IgniteWritersFactory;
import org.alliedium.ignite.migration.dao.IgniteScanner;
import org.alliedium.ignite.migration.dao.IgniteWritersFactoryImpl;
import org.alliedium.ignite.migration.dao.dataaccessor.IIgniteDAO;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteDAO;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.alliedium.ignite.migration.dto.ICacheMetaData;
import org.alliedium.ignite.migration.propeties.PropertiesResolver;
import org.alliedium.ignite.migration.serializer.AvroDeserializer;
import org.alliedium.ignite.migration.serializer.AvroSerializer;
import org.alliedium.ignite.migration.serializer.IAvroDeserializer;
import org.alliedium.ignite.migration.serializer.ISerializer;

import java.nio.file.Path;
import java.util.Map;

import org.apache.ignite.Ignite;

/**
 * Controller is a unit responsible for CLI general commands processing: data serialization (from ignite to avro) and deserialization (from avro to ignite).
 * It requires an ignite connection to be passed in on the initialization stage.
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

    public Controller(Ignite ignite, IgniteAtomicLongNamesProvider atomicNamesProvider) {
        this(ignite, atomicNamesProvider, PropertiesResolver.empty());
    }

    public Controller(Ignite ignite, IgniteAtomicLongNamesProvider atomicNamesProvider, PropertiesResolver propertiesResolver) {
        igniteScanner = new IgniteScanner(atomicNamesProvider, ignite);
        igniteWriter = new IgniteWritersFactoryImpl(ignite);
        igniteDAO = new IgniteDAO(ignite);
        this.dispatcherFactory = new DispatcherFactory(propertiesResolver);
    }

    public void serializeDataToAvro(Path rootSerializedDataPath) {
        ISerializer avroSerializer = new AvroSerializer(rootSerializedDataPath);
        Dispatcher<ICacheMetaData> cacheMetaDataDispatcher = dispatcherFactory.newDispatcher();
        cacheMetaDataDispatcher.subscribe(avroSerializer.getCacheMetaDataSerializer());

        Dispatcher<ICacheData> cacheDataDispatcher = dispatcherFactory.newDispatcher();
        cacheDataDispatcher.subscribe(avroSerializer.getCacheDataSerializer());

        Dispatcher<Map.Entry<String, Long>> atomicsLongDispatcher = dispatcherFactory.newDispatcher();
        atomicsLongDispatcher.subscribe(avroSerializer.getAtomicLongsConsumer());

        Runnable scanner = () -> igniteScanner.read(igniteDAO, cacheMetaDataDispatcher, cacheDataDispatcher, atomicsLongDispatcher);

        TasksExecutor tasksExecutor = new TasksExecutor(scanner, cacheDataDispatcher, cacheMetaDataDispatcher, atomicsLongDispatcher);
        tasksExecutor.execute();
        tasksExecutor.waitForCompletion();
        tasksExecutor.shutdown();
    }

    public void deserializeDataFromAvro(Path avroFilesPath) {
        IAvroDeserializer avroDeserializer = new AvroDeserializer(avroFilesPath);

        Dispatcher<ICacheMetaData> cacheMetaDataDispatcher = dispatcherFactory.newDispatcher();
        cacheMetaDataDispatcher.subscribe(igniteWriter.getIgniteCacheMetaDataWriter());

        Dispatcher<ICacheData> cacheDataDispatcher = dispatcherFactory.newDispatcher();
        cacheDataDispatcher.subscribe(igniteWriter.getIgniteCacheDataWriter());

        Dispatcher<Map.Entry<String, Long>> atomicsLongDispatcher = dispatcherFactory.newDispatcher();
        atomicsLongDispatcher.subscribe(igniteWriter.getIgniteAtomicsLongDataWriter());

        Runnable deserializer = () -> {
            avroDeserializer.deserializeCaches(cacheMetaDataDispatcher, cacheDataDispatcher);
            avroDeserializer.deserializeAtomicsLong(atomicsLongDispatcher);
        };

        TasksExecutor tasksExecutor = new TasksExecutor(deserializer, cacheDataDispatcher, cacheMetaDataDispatcher,
                atomicsLongDispatcher);
        tasksExecutor.execute();
        tasksExecutor.waitForCompletion();
        tasksExecutor.shutdown();
    }
}
