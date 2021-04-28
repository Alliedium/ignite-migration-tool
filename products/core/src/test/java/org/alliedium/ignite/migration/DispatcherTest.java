package org.alliedium.ignite.migration;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.alliedium.ignite.migration.MockitoUtil.doAnswerVoid;
import static org.junit.Assert.*;

public class DispatcherTest {

    public static final int LATCH_TIMEOUT_SECONDS = 30;

    @Test
    @SuppressWarnings("unchecked")
    public void testDispatcher() throws Exception {
        String[] elements = new String[]{"hello", "world", "today", "is", "Thursday"};
        AtomicInteger index = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(elements.length);
        CountDownLatch latchResourceClose = new CountDownLatch(1);
        IDataWriter<String> consumer = Mockito.mock(IDataWriter.class);
        doAnswerVoid(
                invocation -> {
                    assertEquals(elements[index.getAndIncrement()], invocation.getArgument(0));
                    latch.countDown();
                }
        ).when(consumer).write(Mockito.any());
        doAnswerVoid(invocationOnMock -> latchResourceClose.countDown())
                .when(consumer).close();

        Dispatcher<String> dispatcher = new Dispatcher<>();
        dispatcher.subscribe(consumer);

        new Thread(dispatcher).start();

        for (String element : elements) {
            dispatcher.publish(element);
        }
        dispatcher.finish();

        assertTrue(latch.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        assertTrue(latchResourceClose.await(LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS));
    }

    @Test
    public void testDispatcherAndBigData() throws InterruptedException {
        String data = "test_data";
        int recordsCount = 1_000_000;
        CountDownLatch latch = new CountDownLatch(recordsCount);
        IDataWriter<String> consumer = text -> {
            Assert.assertEquals(data, text);
            latch.countDown();
        };

        Dispatcher<String> dispatcher = new Dispatcher<>();
        dispatcher.subscribe(consumer);

        new Thread(dispatcher).start();

        for (int i = 0; i < recordsCount; i++) {
            dispatcher.publish(data);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));
        dispatcher.finish();
    }
}