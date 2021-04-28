package org.alliedium.ignite.migration.propeties;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class PropertiesResolver {

    private final Properties properties;

    private PropertiesResolver(Properties properties) {
        this.properties = properties;
    }

    public List<String> getAtomicLongNamesList() {
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

    public static PropertiesResolver empty() {
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
