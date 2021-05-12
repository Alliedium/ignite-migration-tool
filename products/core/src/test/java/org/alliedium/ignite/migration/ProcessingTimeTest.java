package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.dao.dataaccessor.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.propeties.PropertiesResolver;
import org.alliedium.ignite.migration.test.model.City;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
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

    @BeforeMethod
    public void beforeTestMethod() throws IOException {
        String csvFilePathStr = "./target/processingResults.csv";
        Files.deleteIfExists(Paths.get(csvFilePathStr));
        if (Files.notExists(Paths.get("./target"))) {
            Files.createDirectories(Paths.get("./target"));
        }
        csvFilePath = Files.createFile(Paths.get(csvFilePathStr));;
        Files.write(csvFilePath, "ElementsCount,SecondsToProcess,MillisecondsToProcess\n".getBytes());
        propertiesResolver = mock(PropertiesResolver.class);
        when(propertiesResolver.getDispatchersElementsLimit()).thenReturn(1000);
    }

    @Test
    public void dataProcessingTimeTest() throws IOException {
        // This loop provides a way to gather processing time step by step
        // no processing time will be lost, all the data will be gathered inside csv file
        /*for (int i = 1_000; i <= 100_000; i+=3_000) {
            dataProcessingTimeTest(i);
        }*/

        dataProcessingTimeTest(10_000);
    }

    private void dataProcessingTimeTest(int elementsCount) throws IOException {
        List<City> cityList = clientAPI.createTestCityCacheAndInsertData(cacheName, elementsCount);

        long startTime = System.nanoTime();

        long clearIgniteTime = serializeDeserialize();

        long result = System.nanoTime() - startTime - clearIgniteTime;

        clientAPI.assertIgniteCacheEqualsList(cityList, cacheName);

        logger.info("--------------------- processing time test ---------------------------");
        logger.info("elementsCount = " + elementsCount);
        logger.info("result | nanoseconds: " + result +
                " | milliseconds: " + TimeUnit.NANOSECONDS.toMillis(result) +
                " | seconds: " + TimeUnit.NANOSECONDS.toSeconds(result));

        Files.write(csvFilePath, (elementsCount + "," + TimeUnit.NANOSECONDS.toSeconds(result)
                + "," + TimeUnit.NANOSECONDS.toMillis(result) + "\n").getBytes(), StandardOpenOption.APPEND);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();
    }

    private long serializeDeserialize() {
        Controller controller = new Controller(ignite, IgniteAtomicLongNamesProvider.EMPTY, propertiesResolver);
        controller.serializeDataToAvro(avroTestSet);

        long startTime = System.nanoTime();

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();

        long clearIgniteTime = System.nanoTime() - startTime;

        controller.deserializeDataFromAvro(avroTestSet);

        return clearIgniteTime;
    }
}
