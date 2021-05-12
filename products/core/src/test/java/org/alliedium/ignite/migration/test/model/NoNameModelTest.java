package org.alliedium.ignite.migration.test.model;

import org.alliedium.ignite.migration.ClientIgniteBaseTest;
import org.alliedium.ignite.migration.Controller;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.annotations.Test;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

public class NoNameModelTest extends ClientIgniteBaseTest {

    @Test
    public void testNoNameModel() {
        String cacheName = "NoNameModelCache";
        QueryEntity queryEntity = new QueryEntity()
                .setValueType(NoNameModel.class.getName())
                .setKeyType(Integer.class.getName())
                .addQueryField("byteArray", byte[].class.getName(), null)
                .addQueryField("timestamp", Timestamp.class.getName(), null);

        CacheConfiguration<Integer, NoNameModel> cacheConfiguration = new CacheConfiguration<>();
        cacheConfiguration.setQueryEntities(Collections.singleton(queryEntity));
        cacheConfiguration.setName(cacheName);

        List<NoNameModel> noNameModels = createCacheAndFillWithData(cacheConfiguration,
                () -> new NoNameModel("hello world".getBytes(), new Timestamp(System.currentTimeMillis())), 10);

        Controller controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(avroTestSet);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        controller.deserializeDataFromAvro(avroTestSet);

        clientAPI.assertIgniteCacheEqualsList(noNameModels, cacheName);
    }
}