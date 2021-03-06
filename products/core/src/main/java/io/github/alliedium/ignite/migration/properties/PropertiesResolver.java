package io.github.alliedium.ignite.migration.properties;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class PropertiesResolver {

    private final Properties properties;
    private final Properties systemProperties;

    PropertiesResolver(Properties properties) {
        this.properties = properties;
        this.systemProperties = System.getProperties();
    }

    public List<String> getAtomicLongNames() {
        String atomicLongNames = properties.getProperty(PropertyNames.ATOMIC_LONG_NAMES_PROPERTY);
        if (atomicLongNames == null) {
            return new ArrayList<>();
        }

        return Arrays.asList(atomicLongNames.split(","));
    }

    public int getDispatchersElementsLimit() {
        String limit = properties.getProperty(PropertyNames.DISPATCHERS_ELEMENTS_LIMIT);
        if (limit == null) {
            return DefaultProperties.DISPATCHERS_ELEMENTS_LIMIT;
        }

        return Integer.parseInt(limit);
    }

    public Optional<String> getIgniteAtomicLongNamesProviderClass() {
        String clazz = systemProperties.getProperty(PropertyNames.ATOMIC_LONG_NAMES_CLASS_PROVIDER);
        if (clazz == null) {
            clazz = properties.getProperty(PropertyNames.ATOMIC_LONG_NAMES_CLASS_PROVIDER);
        }

        return Optional.ofNullable(clazz);
    }

    /**
     * @return Returns close ignite instance after run property value.
     *         The value is true by default.
     */
    public boolean closeIgniteInstanceAfterRun() {
        String closeAfterRun = systemProperties.getProperty(PropertyNames.CLOSE_IGNITE_INSTANCE_AFTER_RUN);
        if (closeAfterRun == null) {
            closeAfterRun = properties.getProperty(PropertyNames.CLOSE_IGNITE_INSTANCE_AFTER_RUN);
        }
        return closeAfterRun == null || Boolean.parseBoolean(closeAfterRun);
    }

    public static PropertiesResolver loadProperties() {
        return new PropertiesResolver(new Properties());
    }

    public static PropertiesResolver loadProperties(String filePath) throws IOException {
        Properties properties = new Properties();

        Path path = Paths.get(filePath);
        try (InputStream inputStream = Files.newInputStream(path)) {
            properties.load(inputStream);
        }

        return new PropertiesResolver(properties);
    }
}
