package org.alliedium.ignite.migration.serializer;

import org.alliedium.ignite.migration.*;
import org.alliedium.ignite.migration.util.PathCombine;
import org.alliedium.ignite.migration.dto.ICacheData;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class AvroSerializerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private final String folderName = "avro-serializer-test-path";
    private final String cacheName = "test_cache";

    @Test
    public void testCacheDataSerializationDeserialization() throws Exception {
        File serializeFolder = folder.newFolder(folderName);
        AvroSerializer serializer = new AvroSerializer(serializeFolder.toPath());

        int recordsCount = 100_000;
        try (IDataWriter<ICacheData> cacheDataWriter = serializer.getCacheDataSerializer()) {
            writeTestData(cacheDataWriter, recordsCount);
        }

        AvroFileReader reader = new AvroFileReader(new PathCombine(serializeFolder.toPath()).plus(cacheName));

        IDispatcher<ICacheData> dispatcher = Mockito.mock(IDispatcher.class);

        AtomicInteger resultRecordsCount = new AtomicInteger();

        MockitoUtil.doAnswerVoid(invocation -> {
            ICacheData data = invocation.getArgument(0);
            Assert.assertEquals(cacheName, data.getCacheName());
            resultRecordsCount.incrementAndGet();
        }).when(dispatcher).publish(Mockito.any());

        reader.distributeCacheData(cacheName, TestUtil.getFieldsTypesForTestCacheData(), dispatcher);

        assertEquals(recordsCount, resultRecordsCount.get());
    }

    @Test
    public void testCacheDataSerializationDeserializationDispatcherOn() throws Exception {
        File serializeFolder = folder.newFolder(folderName);
        AvroSerializer serializer = new AvroSerializer(serializeFolder.toPath());

        int recordsCount = 100_000;
        try (IDataWriter<ICacheData> cacheDataWriter = serializer.getCacheDataSerializer()) {
            writeTestData(cacheDataWriter, recordsCount);
        }

        AvroFileReader reader = new AvroFileReader(new PathCombine(serializeFolder.toPath()).plus(cacheName));

        CountDownLatch latch = new CountDownLatch(recordsCount);
        IDataWriter<ICacheData> dataWriter = data -> {
            Assert.assertEquals(cacheName, data.getCacheName());
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