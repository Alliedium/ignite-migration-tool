package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.propeties.PropertyNames;
import org.alliedium.ignite.migration.test.model.City;
import org.apache.ignite.IgniteAtomicLong;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.*;

public class CLITest extends ClientIgniteBaseTest {

    private static final String cacheName = "test_cli_cache";
    private static final Random random = new Random();

    @ClassRule
    public static TemporaryFolder folder = new TemporaryFolder();

    @After
    public void after() {
        System.clearProperty(PropertyNames.System.PROPERTIES_FILE_PATH);
    }

    @Test
    public void serializeDeserializeTest() throws IOException {
        // # test data generation
        List<City> cityList = clientAPI.createTestCityCacheAndInsertData(cacheName, 10);

        Map<String, Long> atomicLongs = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            long val = random.nextLong();
            String key = String.valueOf(val);
            atomicLongs.put(key, val);
            IgniteAtomicLong atomicLong = ignite.atomicLong(key, 0, true);
            atomicLong.getAndSet(val);
        }

        File propsFile = folder.newFile(getRandomPropertiesFileName());
        Properties properties = new Properties();
        properties.setProperty(PropertyNames.ATOMIC_LONG_NAMES_PROPERTY,
                String.join(",", atomicLongs.keySet()));
        try(FileOutputStream outputStream = new FileOutputStream(propsFile)) {
            properties.store(outputStream, null);
        }

        System.setProperty(PropertyNames.System.PROPERTIES_FILE_PATH, propsFile.toPath().toString());

        serializeViaCLI();

        clientAPI.clearIgniteAndCheckIgniteIsEmpty(atomicLongs);

        deserializeViaCLI();

        // # ignite test data check
        clientAPI.assertAtomicLongs(atomicLongs);

        clientAPI.assertIgniteCacheEqualsList(cityList, cacheName);

        clientAPI.closeAtomicLongs(atomicLongs);
    }

    @Test
    public void testDispatcherLimitWorksCorrectly() throws IOException {
        List<City> cityList = clientAPI.createTestCityCacheAndInsertData(cacheName, 100);
        File propsFile = folder.newFile(getRandomPropertiesFileName());
        Properties properties = new Properties();
        properties.setProperty(PropertyNames.DISPATCHERS_ELEMENTS_LIMIT, "10");
        try(FileOutputStream outputStream = new FileOutputStream(propsFile)) {
            properties.store(outputStream, null);
        }

        System.setProperty(PropertyNames.System.PROPERTIES_FILE_PATH, propsFile.toPath().toString());

        serializeViaCLI();

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        deserializeViaCLI();

        clientAPI.assertIgniteCacheEqualsList(cityList, cacheName);
    }

    @Test(expected = NumberFormatException.class)
    public void testWrongFormatForDispatcherElementsLimit() throws IOException {
        File propsFile = folder.newFile(getRandomPropertiesFileName());
        Properties properties = new Properties();
        properties.setProperty(PropertyNames.DISPATCHERS_ELEMENTS_LIMIT, "k10l");
        try (FileOutputStream outputStream = new FileOutputStream(propsFile)) {
            properties.store(outputStream, null);
        }

        System.setProperty(PropertyNames.System.PROPERTIES_FILE_PATH, propsFile.toPath().toString());

        serializeViaCLI();
    }

    private void serializeViaCLI() throws IOException {
        CLI.main(new String[] {
                "--" + PropertyNames.CLI.SERIALIZE,
                "--" + PropertyNames.CLI.PATH, avroTestSet.toString()}, false);
    }

    private void deserializeViaCLI() throws IOException {
        CLI.main(new String[] {
                "--" + PropertyNames.CLI.DESERIALIZE,
                "--" + PropertyNames.CLI.PATH, avroTestSet.toString()}, false);
    }

    private String getRandomPropertiesFileName() {
        return String.format("ignite-migration-tool-%d-%d.properties", random.nextInt(), System.nanoTime());
    }
}