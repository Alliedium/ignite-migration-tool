package org.alliedium.ignite.migration.test.model;

import org.alliedium.ignite.migration.ClientIgniteBaseTest;
import org.alliedium.ignite.migration.Controller;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.configuration.CacheConfiguration;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
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

        IgniteCache<Integer, NoNameModel> igniteCache = ignite.createCache(cacheConfiguration);
        List<NoNameModel> noNameModels = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            noNameModels.add(i, new NoNameModel("hello world".getBytes(), new Timestamp(System.currentTimeMillis())));
            igniteCache.put(i, noNameModels.get(i));
        }

        Controller controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(avroTestSet);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        controller.deserializeDataFromAvro(avroTestSet);

        igniteCache = ignite.cache(cacheName);
        for (int i = 0; i < noNameModels.size(); i++) {
            Assert.assertEquals(noNameModels.get(i), igniteCache.get(i));
        }
    }
}