package io.github.alliedium.ignite.migration;

import io.github.alliedium.ignite.migration.properties.DefaultProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Provides an async implementation of IDispatcher, distributes messages between subscribers and publishers,
 * Accepts elements limit property in order to regulate throughput of entre tool.
 * The throughput is critical if there are RAM limitation.
 * @param <DTO>
 */
public class Dispatcher<DTO> implements IDispatcher<DTO>, Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Dispatcher.class);

    private final List<IDataWriter<DTO>> consumers = new ArrayList<>();
    private final BlockingQueue<DTO> elements;
    private final AtomicBoolean finished = new AtomicBoolean();

    public Dispatcher() {
        this(DefaultProperties.DISPATCHERS_ELEMENTS_LIMIT);
    }

    public Dispatcher(int elementsLimit) {
        elements = new LinkedBlockingQueue<>(elementsLimit);
    }

    @Override
    public void finish() {
        finished.set(true);
    }

    @Override
    public void publish(DTO cacheDTO) {
        checkNotFinished();
        try {
            boolean inserted = elements.offer(cacheDTO, 1, TimeUnit.SECONDS);
            if (!inserted) {
                publish(cacheDTO);
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public void subscribe(IDataWriter<DTO> consumer) {
        checkNotFinished();
        consumers.add(consumer);
    }

    @Override
    public void run() {
        int count = 0;
        long lastProgressShownNanoTime = 0;
        long showProgressInterval = TimeUnit.SECONDS.toNanos(1);
        try {
            while(!finished.get() || !elements.isEmpty()) {
                DTO element = elements.poll(1, TimeUnit.SECONDS);
                if (element != null) {
                    consumers.forEach(consumer -> consumer.write(element));
                    // prints something in order to indicate progress
                    long currentNanoTime = System.nanoTime();
                    count++;
                    if (currentNanoTime - lastProgressShownNanoTime >= showProgressInterval) {
                        lastProgressShownNanoTime = currentNanoTime;
                        System.out.printf(
                                "[PROGRESS INDICATOR] Records processed for the past 1 second: %d\n", count);
                        count = 0;
                    }
                }
            }
        } catch(InterruptedException e) {
            e.printStackTrace();
            logger.error("Dispatching failed due to exception ", e);
            throw new IllegalStateException(e);
        } catch (RuntimeException | Error e) {
            e.printStackTrace();
            logger.error("Dispatching failed due to exception", e);
            throw e;
        } finally {
            closeResources();
        }
    }

    private void checkNotFinished() {
        if (finished.get()) {
            throw new IllegalStateException("Dispatching is finished");
        }
    }

    private void closeResources() {
        try {
            for (IDataWriter<DTO> consumer : consumers) {
                consumer.close();
            }
            consumers.clear();
        } catch(Exception e) {
            logger.error("Failed to close consumers", e);
            throw new IllegalStateException(e);
        } finally {
            finished.set(true);
        }
    }
}
