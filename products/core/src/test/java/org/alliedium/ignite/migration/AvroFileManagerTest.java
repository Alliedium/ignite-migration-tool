package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.serializer.AvroFileReader;
import org.alliedium.ignite.migration.serializer.utils.AvroFileExtensions;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.alliedium.ignite.migration.test.ClientAPI;
import org.alliedium.ignite.migration.util.PathCombine;
import org.alliedium.ignite.migration.test.model.City;
import org.alliedium.ignite.migration.test.DefaultIgniteAtomicLongNamesProvider;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.ignite.IgniteAtomicLong;
import org.apache.ignite.IgniteCache;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

public class AvroFileManagerTest extends ClientIgniteBaseTest {

    @Test
    public void sqlIgniteMigrationToolTest() throws SQLException, IOException {
        ClientAPI.deleteDirectoryRecursively(avroMainPath);
        Path path = Paths.get("src/test/resources/world.sql");
        if (!Files.exists(path)) {
            // path inside docker container
            path = Paths.get("core/src/test/resources/world.sql");
        }

        byte[] sqlFileContent = Files.readAllBytes(path);

        readSqlResourceIntoIgnite(sqlFileContent);

        Controller controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(avroTestSet);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        processDataSetAndCompareWithInitial();
    }

    @Test
    public void igniteAtomicsSerializeAndDeserializeTest() {
        List<String> atomicLongList = Stream.of("test_atomic").collect(Collectors.toList());
        int testAtomicVal = 5;
        IgniteAtomicLong testAtomic = ignite.atomicLong(atomicLongList.get(0), testAtomicVal, true);
        IgniteAtomicLongNamesProvider atomicNamesProvider = () -> atomicLongList;

        Controller controller = new Controller(ignite, atomicNamesProvider);
        controller.serializeDataToAvro(avroTestSet);

        testAtomic.close();
        Assert.assertTrue(testAtomic.removed());

        controller.deserializeDataFromAvro(avroTestSet);

        testAtomic = ignite.atomicLong(atomicLongList.get(0), 0, false);
        Assert.assertEquals(testAtomicVal, testAtomic.get());
        Assert.assertFalse(testAtomic.removed());

        testAtomic.close();
    }

    @Test
    public void igniteAtomicsDefaultNamesProvider() {
        // # data generation for test and ignite filling with the data
        List<String> cacheNames = Stream.of("cache_1", "cache_2").collect(Collectors.toList());
        Map<String, Long> atomicLongDataMap = new HashMap<>();
        cacheNames.forEach(cacheName -> {
            String atomicName1 = "PROD_" + cacheName + "_seq";
            String atomicName2 = cacheName + "_seq";
            atomicLongDataMap.put(atomicName1, random.nextLong());
            atomicLongDataMap.put(atomicName2, random.nextLong());
        });

        atomicLongDataMap.forEach((name, val) -> {
            IgniteAtomicLong atomicLong = ignite.atomicLong(name, 0, true);
            atomicLong.getAndSet(val);
        });

        List<City> testCities = clientAPI.createTestCityCacheAndInsertData(cacheNames.get(0), 3);

        ignite.createCache(cacheNames.get(1));

        // # migration tool: serialization ignite data (snapshot) and writing to files
        Controller controller = new Controller(ignite, new DefaultIgniteAtomicLongNamesProvider(ignite)::getAtomicNames);
        controller.serializeDataToAvro(avroTestSet);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty(atomicLongDataMap);

        // # deserialization of files with ignite data and restoring ignite data
        controller.deserializeDataFromAvro(avroTestSet);

        // # ignite test data check
        clientAPI.assertAtomicLongs(atomicLongDataMap);

        clientAPI.assertIgniteCacheEqualsList(testCities, cacheNames.get(0));

        Assert.assertNotNull(ignite.cache(cacheNames.get(1)));

        clientAPI.closeAtomicLongs(atomicLongDataMap);
    }

    /**
     * Ignored because current ignite migration tool approximately works without query entities
     */
    @Test(
            expectedExceptions = Exception.class,
            expectedExceptionsMessageRegExp = ".*ignite migration tool does not work without ignite cache query entities.*")
    @Ignore
    public void migrationToolDoesNotWorkWithoutQueryEntitiesTest() {
        String cacheName = "migrationToolDoesNotWorkWithoutQueryEntitiesTest";
        IgniteCache<Integer, City> igniteCache = ignite.createCache(cacheName);
        igniteCache.put(0, new City("test_city", "test_district", random.nextInt()));

        Controller controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(avroTestSet);
    }

    public void processDataSetAndCompareWithInitial() throws IOException {
        Controller controller = new Controller(ignite, new DefaultIgniteAtomicLongNamesProvider(ignite)::getAtomicNames);
        controller.deserializeDataFromAvro(avroTestSet);
        controller.serializeDataToAvro(avroMainPath);

        List<Path> initialDataSetSubDirList = Utils.getSubdirectoryPathsFromDirectory(avroTestSet);
        List<Path> resultingDataSetSubDirList = Utils.getSubdirectoryPathsFromDirectory(avroMainPath);

        boolean subDirectoriesAmountMatches = (initialDataSetSubDirList.size() == resultingDataSetSubDirList.size());
        Assert.assertTrue(subDirectoriesAmountMatches);

        boolean areEqual = true;
        for (Path subdirectoryPath : initialDataSetSubDirList) {
            List<String> initialFileNamesList = Utils.getFileNamesFromDirectory("", subdirectoryPath);
            AvroFileReader avroFileReader = new AvroFileReader(new PathCombine(avroMainPath));

            for (String fileName : initialFileNamesList) {
                Path initialFilePath = Paths.get(subdirectoryPath.toString(), fileName);
                Path comparingFilePath = Paths.get(avroMainPath.toString(), initialFilePath.getName(2).toString(), fileName);
                if (fileName.endsWith(AvroFileExtensions.AVRO)) {
                    Schema initialFileSchema = TestUtil.getSchemaForAvroFile(initialFilePath);
                    List<GenericRecord> firstFileDeserializedData = avroFileReader.deserializeAvro(initialFilePath, initialFileSchema);

                    Schema comparingFileSchema = TestUtil.getSchemaForAvroFile(comparingFilePath);
                    List<GenericRecord> secondFileDeserializedData = avroFileReader.deserializeAvro(comparingFilePath, comparingFileSchema);

                    if (!firstFileDeserializedData.equals(secondFileDeserializedData)) {
                        areEqual = TestUtil.compareDeserializedData(firstFileDeserializedData, secondFileDeserializedData);
                    }
                }
                else {
                    areEqual = TestUtil.compareTwoFiles(initialFilePath.toFile(), comparingFilePath.toFile());
                }

                if (!areEqual) {
                    break;
                }
            }
        }

        Assert.assertTrue(areEqual);
    }

    private void readSqlResourceIntoIgnite(byte[] sqlFileContent) throws SQLException {
        String sqlLines = new String(sqlFileContent);
        String[] sqlQueries = sqlLines.split(";");

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1/")) {
            try (Statement stmt = conn.createStatement()) {
                for (String sqlQuery : sqlQueries) {
                    stmt.execute(sqlQuery);
                }
            }
        }
    }
}