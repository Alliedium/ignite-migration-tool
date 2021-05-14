package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.*;
import org.alliedium.ignite.migration.test.ClientAPI;
import org.alliedium.ignite.migration.test.TestDirectories;
import org.alliedium.ignite.migration.util.PathCombine;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class AvroSerializerTest {

    private final String cacheName = "test_cache";
    private Path serializeFolder;

    @BeforeClass
    public void beforeClass() {
        TestDirectories testDirectories = new TestDirectories();
        serializeFolder = testDirectories.getAvroTestSetPath();
    }

    @BeforeMethod
    @AfterMethod
    public void beforeAndAfterMethod() throws IOException {
        ClientAPI.deleteDirectoryRecursively(serializeFolder);
    }

    @Test
    public void testCacheDataSerializationDeserialization() throws Exception {
        AvroSerializer serializer = new AvroSerializer(serializeFolder);

        int recordsCount = 100_000;
        try (IDataWriter<ICacheData> cacheDataWriter = serializer.getCacheDataSerializer()) {
            writeTestData(cacheDataWriter, recordsCount);
        }

        AvroFileReader reader = new AvroFileReader(new PathCombine(serializeFolder).plus(cacheName));

        IDispatcher<ICacheData> dispatcher = Mockito.mock(IDispatcher.class);

        AtomicInteger resultRecordsCount = new AtomicInteger();

        MockitoUtil.doAnswerVoid(invocation -> {
            ICacheData data = invocation.getArgument(0);
            assertEquals(cacheName, data.getCacheName());
            resultRecordsCount.incrementAndGet();
        }).when(dispatcher).publish(Mockito.any());

        reader.distributeCacheData(cacheName, TestUtil.getFieldsTypesForTestCacheData(), dispatcher);

        assertEquals(recordsCount, resultRecordsCount.get());
    }

    @Test
    public void testCacheDataSerializationDeserializationDispatcherOn() throws Exception {
        AvroSerializer serializer = new AvroSerializer(serializeFolder);

        int recordsCount = 100_000;
        try (IDataWriter<ICacheData> cacheDataWriter = serializer.getCacheDataSerializer()) {
            writeTestData(cacheDataWriter, recordsCount);
        }

        AvroFileReader reader = new AvroFileReader(new PathCombine(serializeFolder).plus(cacheName));

        CountDownLatch latch = new CountDownLatch(recordsCount);
        IDataWriter<ICacheData> dataWriter = data -> {
            assertEquals(cacheName, data.getCacheName());
            latch.countDown();
        };

        Dispatcher<ICacheData> dispatcher = new Dispatcher<>();
        dispatcher.subscribe(dataWriter);

        new Thread(dispatcher).start();

        reader.distributeCacheData(cacheName, TestUtil.getFieldsTypesForTestCacheData(), dispatcher);

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        dispatcher.finish();
    }

    private void writeTestData(IDataWriter<ICacheData> writer, int recordsCount) {
        for (int count = 0; count < recordsCount; count++) {
            writer.write(TestUtil.createTestCacheData(cacheName));
        }
    }
}