package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.dao.converters.IIgniteDTOConverter;
import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.dto.CacheConfigurationData;
import org.alliedium.ignite.migration.dto.CacheDataTypes;
import org.alliedium.ignite.migration.dto.CacheEntryMetaData;
import org.alliedium.ignite.migration.dto.CacheMetaData;
import org.alliedium.ignite.migration.serializer.AvroCacheConfigurationsWriter;
import org.alliedium.ignite.migration.serializer.AvroFileReader;
import org.alliedium.ignite.migration.serializer.AvroFileWriter;
import org.alliedium.ignite.migration.serializer.utils.AvroFileNames;
import org.alliedium.ignite.migration.test.model.PartialField;
import org.alliedium.ignite.migration.test.model.PartialFieldBinarylizable;
import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class SupportedActionsQueryEntityTest extends ClientIgniteBaseTest {

    private Controller controller;

    @BeforeMethod
    public void supportedActionsQueryEntityBefore() {
        controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
    }

    /**
     * This test shows that ignite migration tool does not work with partial query entities
     * if standard ignite marshalling is used (no {@link org.apache.ignite.binary.Binarylizable} usage).
     * All fields of a cache should have query entities in order to be available for serialization.
     * This is so cause ignite migration tool highly relies on fields types to properly serialize and
     * deserialize data.
     * since this change tool is approximately able to work without query entities for fields
     */
    @Ignore
    @Test(
            expectedExceptions = Exception.class,
            expectedExceptionsMessageRegExp = ".*Query entity for field \\[value\\] was not found.*")
    public void partialQueryEntitiesTest() {
        String cacheName = "partialQueryEntitiesTest";
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(PartialField.class.getName())
                .setKeyType(Integer.class.getName())
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("type", String.class.getName(), null);

        CacheConfiguration<Integer, PartialField> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
        cacheConfiguration.setName(cacheName);

        clientAPI.createCacheAndFillWithData(cacheConfiguration,
                () -> new PartialField("testName", "testType", "testVal"), 10);

        controller.serializeDataToAvro(clientAPI.getAvroTestSetPath());
    }

    /**
     * This test shows partial Query entity deserialization,
     * when serialized query entity differs from that one which is inside ignite.
     * Inside this test a query field is removed from serialized query entity,
     * after that all serialized data is being deserialized and written to ignite.
     * This works because ignite contains initial query entity.
     * @throws IOException
     */
    @Test
    public void partialQueryEntitiesDeserializeTest() {
        String cacheName = "partialQueryEntitiesDeserializeTest";
        QueryEntity queryEntity = createStandardQueryEntity();

        CacheConfiguration<Integer, PartialField> cacheConfiguration = createStandardCacheConfiguration(queryEntity, cacheName);
        List<PartialField> partialFields = createDataAndSerialize(cacheConfiguration);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        removeFieldFromSerializedQueryEntity(cacheConfiguration, "value");

        controller.deserializeDataFromAvro(clientAPI.getAvroTestSetPath());

        clientAPI.assertIgniteCacheEqualsList(partialFields, cacheName);
    }

    /**
     * This test shows that tool will work properly in case query entities are not declared
     * for all fields of cache object in ignite, but are declared in serialized data.
     */
    @Test
    public void partialQueryEntitiesInAllSidesDeserializeTest() {
        String cacheName = "partialQueryEntitiesInAllSidesDeserializeTest";
        String fieldName = "value";
        String cacheNameSecond = cacheName + 1;
        QueryEntity queryEntity = createStandardQueryEntity();

        CacheConfiguration<Integer, PartialField> cacheConfiguration = createStandardCacheAndSerialize(queryEntity, cacheName);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        removeFieldFromQueryEntityIgnite(cacheConfiguration, fieldName);
        removeFieldFromSerializedQueryEntity(cacheConfiguration, fieldName);

        cacheConfiguration.setName(cacheNameSecond);

        ignite.createCache(cacheConfiguration);

        controller.deserializeDataFromAvro(clientAPI.getAvroTestSetPath());

        QueryEntity updatedQueryEntity = (QueryEntity) ignite.cache(cacheNameSecond).getConfiguration(CacheConfiguration.class)
                .getQueryEntities().iterator().next();
        Assert.assertEquals(2, updatedQueryEntity.getFields().size());
        Assert.assertNull(updatedQueryEntity.getFields().get(fieldName));

        ignite.cache(cacheNameSecond).withKeepBinary()
                .query(new ScanQuery<Object, BinaryObject>())
                .forEach(entry -> {
                    Objects.requireNonNull(entry.getValue().field(fieldName), "ignite cache objects should contain new field");
                });
    }

    /**
     * This test shows that ignite migration tool works with custom marshalling {@link org.apache.ignite.binary.Binarylizable}.
     * All fields mentioned in methods {@link org.apache.ignite.binary.Binarylizable#readBinary} and
     * {@link org.apache.ignite.binary.Binarylizable#writeBinary} should have query entities.
     */
    @Test
    public void partialBinarylizableTest() {
        String cacheName = "partialBinarylizableTest";
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(PartialFieldBinarylizable.class.getName())
                .setKeyType(Integer.class.getName())
                .addQueryField("specialName", String.class.getName(), null)
                .addQueryField("specialType", String.class.getName(), null)
                .addQueryField("specialValue", String.class.getName(), null);

        CacheConfiguration<Integer, PartialFieldBinarylizable> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
        cacheConfiguration.setName(cacheName);

        List<PartialFieldBinarylizable> partialFields = clientAPI.createCacheAndFillWithData(cacheConfiguration,
                () -> new PartialFieldBinarylizable("testName", "testType", "testVal", "testConsumer"), 10);

        controller.serializeDataToAvro(clientAPI.getAvroTestSetPath());

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        controller.deserializeDataFromAvro(clientAPI.getAvroTestSetPath());

        clientAPI.assertIgniteCacheEqualsList(partialFields, cacheName);
    }

    /**
     * This test is bad path test and shows that tool will fail in case fields mentioned in methods
     * {@link org.apache.ignite.binary.Binarylizable#readBinary} and {@link org.apache.ignite.binary.Binarylizable#writeBinary}
     * do not have query entities.
     * since this change tool is approximately able to work without query entities for fields
     */
    @Ignore
    @Test(
            expectedExceptions = Exception.class,
            expectedExceptionsMessageRegExp = ".*Query entity for field \\[specialValue\\] was not found.*")
    public void partialBinarylizableTestBadPath() {
        String cacheName = "partialBinarylizableTestBadPath";
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(PartialFieldBinarylizable.class.getName())
                .setKeyType(Integer.class.getName())
                .addQueryField("specialName", String.class.getName(), null)
                .addQueryField("specialType", String.class.getName(), null);

        CacheConfiguration<Integer, PartialFieldBinarylizable> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
        cacheConfiguration.setName(cacheName);

        clientAPI.createCacheAndFillWithData(cacheConfiguration,
                () -> new PartialFieldBinarylizable("testName", "testType", "testVal", "testConsumer"), 10);

        controller.serializeDataToAvro(clientAPI.getAvroTestSetPath());
    }

    private QueryEntity createStandardQueryEntity() {
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(PartialField.class.getName())
                .setKeyType(Integer.class.getName())
                .addQueryField("name", String.class.getName(), null)
                .addQueryField("type", String.class.getName(), null)
                .addQueryField("value", String.class.getName(), null);
        return queryEntity;
    }

    private CacheConfiguration<Integer, PartialField> createStandardCacheAndSerialize(QueryEntity queryEntity, String cacheName) {
        CacheConfiguration<Integer, PartialField> cacheConfiguration = createStandardCacheConfiguration(queryEntity, cacheName);

        createDataAndSerialize(cacheConfiguration);

        return cacheConfiguration;
    }

    private CacheConfiguration<Integer, PartialField> createStandardCacheConfiguration(QueryEntity queryEntity, String cacheName) {
        CacheConfiguration<Integer, PartialField> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
        cacheConfiguration.setName(cacheName);
        return cacheConfiguration;
    }

    private List<PartialField> createDataAndSerialize(CacheConfiguration<Integer, PartialField> cacheConfiguration) {
        List<PartialField> partialFields = clientAPI.createCacheAndFillWithData(cacheConfiguration,
                () -> new PartialField("testName", "testType", "testVal"), 10);

        controller.serializeDataToAvro(clientAPI.getAvroTestSetPath());
        return partialFields;
    }

    private void removeFieldFromQueryEntityIgnite(CacheConfiguration<?, ?> cacheConfiguration, String fieldName) {
        QueryEntity queryEntity = cacheConfiguration.getQueryEntities().iterator().next();
        LinkedHashMap<String, String> fields = queryEntity.getFields();
        fields.remove(fieldName);
        queryEntity.setFields(fields);
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
    }

    private void removeFieldFromSerializedQueryEntity(CacheConfiguration<Integer, PartialField> cacheConfiguration,
                                                      String fieldName) {
        String cacheName = cacheConfiguration.getName();
        Path source = clientAPI.getAvroTestSetPath();
        IIgniteDTOConverter<String, Collection<QueryEntity>> queryEntityConverter = IgniteObjectStringConverter.QUERY_ENTITY_CONVERTER;
        IIgniteDTOConverter<String, Object> converter = IgniteObjectStringConverter.GENERIC_CONVERTER;

        AvroFileReader reader = new AvroFileReader(new PathCombine(source).plus(cacheName));

        Collection<QueryEntity> queryEntities = queryEntityConverter.convertFromDTO(reader.getCacheEntryMeta());
        QueryEntity innerQueryEntity = queryEntities.iterator().next();
        LinkedHashMap<String, String> fields = innerQueryEntity.getFields();
        fields.remove(fieldName);
        innerQueryEntity.setFields(fields);

        cacheConfiguration.setQueryEntities(queryEntities);
        Path cacheConfigurationsAvroFilePath = new PathCombine(source).plus(AvroFileNames.CACHE_CONFIGURATION_FILENAME).getPath();
        AvroCacheConfigurationsWriter cacheConfigurationsWriter = new AvroCacheConfigurationsWriter.Builder()
                .setAvroSchema(reader.getCacheConfigurationsAvroSchema())
                .setCacheConfigurationsAvroFilePath(cacheConfigurationsAvroFilePath)
                .setCacheConfigurations(converter.convertFromEntity(cacheConfiguration))
                .setCacheQueryEntities(queryEntityConverter.convertFromEntity(queryEntities))
                .setCacheDataTypes(new CacheDataTypes(innerQueryEntity.getKeyType(), innerQueryEntity.getValueType()))
                .build();
        cacheConfigurationsWriter.write();
    }
}
