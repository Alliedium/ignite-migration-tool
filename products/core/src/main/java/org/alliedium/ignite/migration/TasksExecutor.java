package org.alliedium.ignite.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TasksExecutor {
    private static final Logger log = LoggerFactory.getLogger(TasksExecutor.class);
    private final Runnable[] tasks;
    private final ExecutorService executorService;
    private final List<Future<?>> futures;

    public TasksExecutor(Runnable... tasks) {
        this.tasks = tasks;
        futures = new ArrayList<>();
        executorService = Executors.newFixedThreadPool(tasks.length);
    }

    public static TasksExecutor execute(Runnable... tasks) {
        return new TasksExecutor(tasks).execute();
    }

    public TasksExecutor execute() {
        for (Runnable task : tasks) {
            futures.add(executorService.submit(task));
        }
        return this;
    }

    public void waitForCompletion() {
        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("One of task finished with errors ", e);
                throw new IllegalStateException(e);
            } finally {
                shutdown();
            }
        });
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
