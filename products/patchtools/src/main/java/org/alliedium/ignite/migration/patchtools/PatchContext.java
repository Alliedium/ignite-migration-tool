package org.alliedium.ignite.migration.patchtools;

import org.alliedium.ignite.migration.Utils;
import org.alliedium.ignite.migration.serializer.CacheAvroFilesLocator;
import org.alliedium.ignite.migration.util.PathCombine;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PatchContext {
    private final Logger logger = LoggerFactory.getLogger(PatchContext.class);

    private final PathCombine rootPath;
    private final PathCombine outputRootPath;
    private final Pipeline pipeline;
    private final Map<String, CacheComponent> cacheComponents = new HashMap<>();
    private final Set<String> markedAsResolvedCaches = new HashSet<>();

    public PatchContext(PathCombine rootPath, PathCombine outputRootPath) {
        this.rootPath = rootPath;
        this.outputRootPath = outputRootPath;
        PipelineOptions options = PipelineOptionsFactory.create();
        pipeline = Pipeline.create(options);
    }

    public void prepare() {
        Set<PathCombine> cachesPaths = Utils.getSubdirectoryPathsFromDirectory(rootPath.getPath())
                .stream().map(PathCombine::new).collect(Collectors.toSet());
        cachesPaths.forEach(cachePath -> {
            cacheComponents.put(cachePath.getPath().toString(), new CacheComponent(cachePath));
        });
    }

    /**
     * Marks cache as resolved for future computations.
     * It will trow an exception if cache will not be found.
     * @param cacheName
     */
    public void markCacheResolved(String cacheName) {
        getCacheComponent(cacheName);
        markedAsResolvedCaches.add(cacheName);
    }

    /**
     * Finds cache component for cache name, the cache name can represent a path to cache and can be simple cache name,
     * it is expected by our structure that directories have the same name as caches and obviously cache name
     * should match the end of cache path.
     * @param cacheName
     * @return cache component
     */
    public CacheComponent getCacheComponent(String cacheName) {
        CacheComponent component = cacheComponents.get(cacheName);
        if (component != null) {
            return component;
        }
        List<String> cacheComponentsPath = cacheComponents.keySet().stream()
                .filter(path -> path != null && path.endsWith(cacheName)).collect(Collectors.toList());
        if (cacheComponentsPath.size() == 1) {
            return cacheComponents.get(cacheComponentsPath.get(0));
        }
        if (cacheComponentsPath.size() > 1) {
            throw new IllegalArgumentException(String.format(
                    "More than one cache was found for the needle [%s], found caches [%s]", cacheName, cacheComponentsPath));
        }

        throw new IllegalArgumentException(String.format("Cache with name %s was not found", cacheName));
    }

    public List<String> selectCachePathsWhichEndWith(String endOfCacheNames) {
        return cacheComponents.keySet().stream()
                .filter(path -> path.endsWith(endOfCacheNames)).collect(Collectors.toList());
    }

    public Pipeline getPipeline() {
        return pipeline;
    }

    public void copyAllNotTouchedFilesToOutput() {
        List<Path> rootPathFiles = readAllSubDirectoryFiles(rootPath.getPath());

        rootPathFiles.forEach(path -> {
            try {
                String strPath = path.toString();
                String fileName = strPath.substring(path.getParent().toString().length() + 1);
                if (markedAsResolvedCaches.contains(fileName)) {
                    return;
                }
                PathCombine destinationPath = outputRootPath.plus(fileName);
                if (!Files.exists(destinationPath.getPath())) {
                    if (Files.isDirectory(path)) {
                        FileUtils.copyDirectory(path.toFile(), destinationPath.getPath().toFile());
                    } else {
                        Files.copy(path, destinationPath.getPath());
                    }
                    logger.info(
                            String.format("File or directory [%s] was copied " +
                                    "into destination directory", destinationPath.getPath()));
                } else {
                    logger.info(
                            String.format("File or directory [%s] was NOT copied because " +
                                    "already exists in destination directory", destinationPath.getPath()));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private List<Path> readAllSubDirectoryFiles(Path directoryPath) {
        try (Stream<Path> stream = Files.list(directoryPath)) {
            return stream
                    .filter(path -> Files.isDirectory(path) || Files.isRegularFile(path))
                    .collect(Collectors.toList());
        }
        catch (IOException exception) {
            throw new RuntimeException(exception);
        }
    }

    public void patchCachesWhichEndWith(String cachesEndWith, Consumer<String> patch) {
        List<String> cachePaths = selectCachePathsWhichEndWith(cachesEndWith);
        AtomicInteger counter = new AtomicInteger();
        cachePaths.forEach(cachePath -> {
            boolean allFilesExist = checkCacheFilesExist(getCacheComponent(cachePath));
            if (!allFilesExist) {
                logger.warn(String.format(
                        "Cache %s was skipped cause not all files exist for this cache, that's why it is invalid", cachePath));
                counter.incrementAndGet();
                return;
            }

            patch.accept(cachePath);
        });
        logger.info("MISSED CACHES COUNTER " + counter.get());
    }

    private boolean checkCacheFilesExist(CacheComponent cacheComponent) {
        CacheAvroFilesLocator filesLocator = cacheComponent.getFilesLocator();
        List<Path> pathsWhichDoNotExist = Stream.of(filesLocator.cacheDataPath(), filesLocator.cacheConfigurationPath(),
                        filesLocator.cacheConfigurationSchemaPath(), filesLocator.cacheDataSchemaPath())
                .filter(path -> !Files.exists(path))
                .collect(Collectors.toList());

        if (pathsWhichDoNotExist.size() > 0) {
            logger.warn("The following cache files where not found " + pathsWhichDoNotExist);
        }

        return pathsWhichDoNotExist.size() == 0;
    }
}
