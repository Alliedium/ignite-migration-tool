package io.github.alliedium.ignite.migration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.*;

public class TasksExecutor {
    private static final Logger log = LoggerFactory.getLogger(TasksExecutor.class);
    private final Runnable[] tasks;
    private final ExecutorService executorService;
    private final List<Future<?>> futures;
    private final CopyOnWriteArrayList<AutoCloseable> resourcesToCloseOnShutdown;

    public TasksExecutor(Runnable... tasks) {
        this.tasks = tasks;
        futures = new ArrayList<>();
        executorService = Executors.newFixedThreadPool(tasks.length);
        resourcesToCloseOnShutdown = new CopyOnWriteArrayList<>();
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

    public TasksExecutor registerResourcesToCloseOnShutdown(AutoCloseable... autoCloseables) {
        resourcesToCloseOnShutdown.addAll(Arrays.asList(autoCloseables));
        return this;
    }

    public void waitForCompletion() {
        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                log.error("One of task finished with errors ", e);
                shutdown();
                throw new IllegalStateException(e);
            }
        });
        shutdown();
    }

    private void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                shutDownNow();
            }
        } catch (InterruptedException e) {
            shutDownNow();
        } finally {
            resourcesToCloseOnShutdown.forEach(resource -> {
                try {
                    resource.close();
                } catch (Exception e) {
                    log.error("Provided resource cannot be closed properly: " + resource + ". " + e);
                }
            });
        }
    }

    private void shutDownNow() {
        List<Runnable> interruptedTasks = executorService.shutdownNow();
        log.error("Executor service shut downed not gracefully. Interrupted tasks count = " + interruptedTasks.size());
    }
}
