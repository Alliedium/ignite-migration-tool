package io.github.alliedium.ignite.migration;

import com.thoughtworks.xstream.XStream;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Utils {

    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    private static final String[] UNWANTED_CACHE_CONFIG_FIELDS_LIST = {"affMapper", "nodeFilter", "pluginCfgs", "qryEntities", "keyCfg", "expiryPolicyFactory"};

    private static final XStream xstream = new XStream();

    static {
        xstream.allowTypesByWildcard(new String[]{"*.**"});
    }

    /**
     * Can return null in case provided object is null, this happens when an object needs to be present
     * even if it's value is null
     * @param object - the object which will be serialized to XML
     * @return serialized object (string)
     */
    public static String serializeObjectToXML(Object object) {
        if (object == null) {
            return null;
        }

        if (object.getClass().equals(org.apache.ignite.configuration.CacheConfiguration.class)) {

            for (String unwantedCacheConfigField : UNWANTED_CACHE_CONFIG_FIELDS_LIST) {
                xstream.omitField(org.apache.ignite.configuration.CacheConfiguration.class, unwantedCacheConfigField);
            }
        }
        return xstream.toXML(object);
    }

    /**
     * Can return null in case provided xml is null or empty, this happens when an object needs to be present
     * even if it's value is null
     * @param xml - string
     * @return object of required type or throws exception in case type mismatched
     */
    public static <T> T deserializeFromXML(String xml) {
        if (xml == null || xml.trim().isEmpty()) {
            return null;
        }
        // todo: throw detailed exception: cache name and key
        //noinspection unchecked
        return (T) xstream.fromXML(xml);
    }

    public static Map<String, String> capitalizeMapKeys(Map<String, String> initialMap) {
        return initialMap.keySet().stream().collect(Collectors.toMap(String::toUpperCase, initialMap::get));
    }

    public static List<String> getFileNamesFromDirectory(String fileNamePrefix, Path directoryPath) throws IOException {
        List<String> matchingFileNamesList = new ArrayList<>();
        Files.walkFileTree(directoryPath, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                if (!Files.isDirectory(file)) {
                    String fileName = file.getFileName().toString();
                    if (fileName.startsWith(fileNamePrefix)) {
                        matchingFileNamesList.add(fileName);
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });

        return matchingFileNamesList;
    }

    public static List<Path> getSubdirectoryPathsFromDirectory(Path directoryPath) {
        List<Path> subdirectoryNamesList = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directoryPath)) {
            for (Path path : stream) {
                if (Files.isDirectory(path)) {
                    subdirectoryNamesList.add(path);
                }
            }
        }
        catch (IOException exception) {
            logger.error("Listing subdirectories from " + subdirectoryNamesList + " failed with an error: " + exception.getMessage());
            throw new RuntimeException(exception);
        }
        return subdirectoryNamesList;
    }

    public static void createFileFromPath(Path filePath) {
        if(Files.exists(filePath)) {
            return;
        }

        try {
            Path parent = filePath.getParent();
            if (parent != null && !Files.exists(parent)) {
                logger.info("parent directory " + parent + " does not exist. Creating the directory.");
                Files.createDirectories(parent);
            }
            Files.createFile(filePath);
        } catch(IOException e) {
            logger.error("Failed to create file by path: " + filePath, e);
            throw new IllegalArgumentException(e);
        }
    }
}