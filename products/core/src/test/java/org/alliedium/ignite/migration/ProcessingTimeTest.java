package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.propeties.PropertiesResolver;
import org.alliedium.ignite.migration.test.model.City;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.configuration.CacheConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.TimeUnit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * The purpose of this test is - gathering execution/processing time of all Apache Ignite migration tool.
 * It writes in logs gathered data and also into the csv file: /target/processingResults.csv.
 * The data from csv could be used in office excel or other csv readers in order to build diagrams or
 * other usable stuff which will make more easy/obvious processing time investigation.
 */
public class ProcessingTimeTest extends ClientIgniteBaseTest {

    protected static final Logger logger = LoggerFactory.getLogger(ProcessingTimeTest.class);
    private String cacheName = "processing_time_test";
    private Path csvFilePath;
    private PropertiesResolver propertiesResolver;
    private Controller controller;

    @BeforeMethod
    public void beforeTestMethod() throws IOException {
        String csvFilePathStr = "./target/processingResults.csv";
        Files.deleteIfExists(Paths.get(csvFilePathStr));
        if (Files.notExists(Paths.get("./target"))) {
            Files.createDirectories(Paths.get("./target"));
        }
        csvFilePath = Files.createFile(Paths.get(csvFilePathStr));;
        String columns = "ElementsCount," +
                "total," +
                "ignite -> avro," +
                "avro -> ignite\n";
        Files.write(csvFilePath, columns.getBytes());
        propertiesResolver = mock(PropertiesResolver.class);
        when(propertiesResolver.getDispatchersElementsLimit()).thenReturn(1000);
        controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY, propertiesResolver);
    }

    @Test
    public void dataProcessingTimeTest() throws IOException {
        // This loop provides a way to gather processing time step by step
        // no processing time will be lost, all the data will be gathered inside csv file
        //for (int i = 50_000; i <= 1_000_000; i+=50_000) {
        //    dataProcessingTimeTest(i);
        //}

        dataProcessingTimeTest(50_000);
    }

    private void dataProcessingTimeTest(int elementsCount) throws IOException {
        CacheConfiguration<Integer, City> cacheConfiguration = clientAPI.createTestCityCacheConfiguration(cacheName);
        IgniteCache<Integer, City> igniteCache = ignite.createCache(cacheConfiguration);
        try (IgniteDataStreamer<Integer, City> dataStreamer = ignite.dataStreamer(cacheName)) {
            for (int cityIndex = 0; cityIndex < elementsCount; cityIndex++) {
                dataStreamer.addData(cityIndex, new City("test_city" + cityIndex, "test_district", random.nextInt()));
            }
        }

        long startTime = System.nanoTime();

        long serializeTime = serialize();

        long clearIgniteTime = clearIgnite();

        long deserializeTime = deserialize();

        long result = System.nanoTime() - startTime - clearIgniteTime;

        Assert.assertEquals(igniteCache.size(), elementsCount);

        logger.info("--------------------- processing time test ---------------------------");
        logger.info("elementsCount = " + elementsCount);
        logger.info("result | nanoseconds: " + result +
                " | milliseconds: " + TimeUnit.NANOSECONDS.toMillis(result) +
                " | seconds: " + TimeUnit.NANOSECONDS.toSeconds(result));

        String record = elementsCount + ","
                + TimeUnit.NANOSECONDS.toMillis(result) + ","
                + TimeUnit.NANOSECONDS.toMillis(serializeTime) + ","
                + TimeUnit.NANOSECONDS.toMillis(deserializeTime) + "\n";

        Files.write(csvFilePath, record.getBytes(), StandardOpenOption.APPEND);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();
    }

    private long serialize() {
        long serializeStartTime = System.nanoTime();
        controller.serializeDataToAvro(avroTestSet);
        return System.nanoTime() - serializeStartTime;
    }

    private long clearIgnite() {
        long clearIgniteStartTime = System.nanoTime();
        clientAPI.clearIgniteAndCheckIgniteIsEmpty();
        return System.nanoTime() - clearIgniteStartTime;
    }

    private long deserialize() {
        long deserializeStartTime = System.nanoTime();
        controller.deserializeDataFromAvro(avroTestSet);
        return System.nanoTime() - deserializeStartTime;
    }
}
