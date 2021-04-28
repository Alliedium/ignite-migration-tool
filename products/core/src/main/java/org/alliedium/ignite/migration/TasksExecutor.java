package org.alliedium.ignite.migration;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TasksExecutor {
    private final Runnable[] tasks;
    private final ExecutorService executorService;
    private final List<Future<?>> futures;

    public TasksExecutor(Runnable... tasks) {
        this.tasks = tasks;
        futures = new ArrayList<>();
        executorService = Executors.newFixedThreadPool(tasks.length);
    }

    public void execute() {
        for (Runnable task : tasks) {
            futures.add(executorService.submit(task));
        }
    }

    public void waitForCompletion() {
        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
                throw new IllegalStateException(e);
            }
        });
    }

    public void shutdown() {
        executorService.shutdown();
    }
}
