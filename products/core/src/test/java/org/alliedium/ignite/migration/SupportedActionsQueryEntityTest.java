package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.dao.converters.IgniteObjectStringConverter;
import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.dao.dtobuilder.CacheConfigBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.EntryMetaBuilder;
import org.alliedium.ignite.migration.dao.dtobuilder.IDTOBuilder;
import org.alliedium.ignite.migration.dto.*;
import org.alliedium.ignite.migration.patchtools.IPatch;
import org.alliedium.ignite.migration.patchtools.PatchProcessor;
import org.alliedium.ignite.migration.test.model.PartialField;
import org.alliedium.ignite.migration.test.model.PartialFieldBinarylizable;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.ScanQuery;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class SupportedActionsQueryEntityTest extends ClientIgniteBaseTest {

    private Controller controller;

    @BeforeMethod
    public void supportedActionsQueryEntityBefore() throws IOException {
        controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
        clientAPI.deleteDirectoryRecursively(clientAPI.getAvroMainPath());
    }

    @AfterMethod
    public void supportedActionsQueryEntityAfter() throws IOException {
        clientAPI.deleteDirectoryRecursively(clientAPI.getAvroMainPath());
    }

    /**
     * This test shows that ignite migration tool does not work with partial query entities
     * if standard ignite marshalling is used (no {@link org.apache.ignite.binary.Binarylizable} usage).
     * All fields of a cache should have query entities in order to be available for serialization.
     * This is so cause ignite migration tool highly relies on fields types to properly serialize and
     * deserialize data.
     */
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

        createCacheAndFillWithData(cacheConfiguration,
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
    public void partialQueryEntitiesDeserializeTest() throws IOException {
        String cacheName = "partialQueryEntitiesDeserializeTest";
        QueryEntity queryEntity = createStandardQueryEntity();

        CacheConfiguration<Integer, PartialField> cacheConfiguration = createStandardCacheConfiguration(queryEntity, cacheName);
        List<PartialField> partialFields = createDataAndSerialize(cacheConfiguration);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        clientAPI.deleteDirectoryRecursively(clientAPI.getAvroMainPath());

        patchSerializedMetaDataRemoveFieldFromQueryEntity(cacheConfiguration, "value");

        controller.deserializeDataFromAvro(clientAPI.getAvroMainPath());

        clientAPI.assertIgniteCacheEqualsList(partialFields, cacheName);
        clientAPI.deleteDirectoryRecursively(clientAPI.getAvroMainPath());
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

        cacheConfiguration.setName(cacheNameSecond);

        ignite.createCache(cacheConfiguration);

        patchSerializedMetaDataRemoveFieldFromQueryEntity(cacheConfiguration, fieldName);

        controller.deserializeDataFromAvro(clientAPI.getAvroMainPath());

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

        List<PartialFieldBinarylizable> partialFields = createCacheAndFillWithData(cacheConfiguration,
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
     */
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

        createCacheAndFillWithData(cacheConfiguration,
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
        List<PartialField> partialFields = createCacheAndFillWithData(cacheConfiguration,
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

    private void patchSerializedMetaDataRemoveFieldFromQueryEntity(CacheConfiguration<Integer, PartialField> cacheConfiguration,
                                                                   String fieldName) {
        String cacheName = cacheConfiguration.getName();

        Path source = clientAPI.getAvroTestSetPath();
        Path destination = clientAPI.getAvroMainPath();
        IgniteObjectStringConverter converter = new IgniteObjectStringConverter();

        IPatch patch = new IPatch() {
            @Override
            public ICacheMetaData transformMetaData(ICacheMetaData metaData) {
                Collection<QueryEntity> queryEntities = (Collection<QueryEntity>)
                        converter.convertFromDto(metaData.getEntryMeta().toString());
                QueryEntity innerQueryEntity = queryEntities.iterator().next();
                LinkedHashMap<String, String> fields = innerQueryEntity.getFields();
                fields.remove(fieldName);
                innerQueryEntity.setFields(fields);

                IDTOBuilder<ICacheEntryMetaData> cacheEntryMetaBuilder = new EntryMetaBuilder(queryEntities, converter);
                IDTOBuilder<ICacheConfigurationData> cacheConfigurationBuilder = new CacheConfigBuilder(cacheConfiguration, converter);

                return new CacheMetaData(cacheName, cacheConfigurationBuilder.build(), cacheEntryMetaBuilder.build());
            }

            @Override
            public ICacheData transformData(ICacheData cacheData) {
                List<ICacheEntryValueField> fields = cacheData.getCacheEntryValue().getFields();
                List<ICacheEntryValueField> resultFields = new ArrayList<>();
                fields.forEach(field ->
                        resultFields.add(new CacheEntryValueField(field.getName(),
                                TypesResolver.toAvroType(field.getTypeClassName()), field.getFieldValue())));

                return new CacheData(cacheName, cacheData.getCacheEntryKey(),
                        new CacheEntryValue(resultFields));
            }
        };

        PatchProcessor processor = new PatchProcessor(source, destination, patch);

        processor.process();
    }
}
