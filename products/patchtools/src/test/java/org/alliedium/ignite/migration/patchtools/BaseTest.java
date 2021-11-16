package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.Controller;
import org.alliedium.ignite.migration.IgniteConfigLoader;
import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.test.ClientAPI;
import org.alliedium.ignite.migration.test.model.City;
import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.ignite.configuration.CacheConfiguration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;

public class BaseTest {

    protected ClientAPI clientAPI;
    protected PathCombine source;
    protected PathCombine destination;
    protected Controller controller;

    @BeforeMethod
    public void beforeMethod() throws IOException {
        clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        source = new PathCombine(clientAPI.getAvroTestSetPath());
        destination = new PathCombine(clientAPI.getAvroMainPath());
        controller = new Controller(clientAPI.getIgnite(), IgniteAtomicLongNamesProvider.EMPTY);
        clientAPI.cleanIgniteAndRemoveDirectories();
        beforeAndAfter();
    }

    @AfterMethod
    public void afterMethod() throws IOException {
        beforeAndAfter();
    }

    private void beforeAndAfter() throws IOException {
        if (clientAPI != null) {
            ClientAPI.deleteDirectoryRecursively(source.getPath());
            ClientAPI.deleteDirectoryRecursively(destination.getPath());
        }
    }

    protected void createCityCache(String cacheName) {
        CacheConfiguration<Integer, City> cityConfiguration = clientAPI.createTestCityCacheConfiguration(cacheName);
        clientAPI.createCacheAndFillWithData(cityConfiguration,
                () -> new City("testName", "testDistrict", clientAPI.getRandom().nextInt()), 10);
    }
}
