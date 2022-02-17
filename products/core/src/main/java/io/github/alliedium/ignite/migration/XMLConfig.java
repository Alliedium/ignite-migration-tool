package io.github.alliedium.ignite.migration;

import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.ignite.Ignition;

/**
 * Current class is used by {@link IgniteConfigLoader} for reading the Apache Ignite configurations represented as XML in file.
 * File location is defined by 'IGNITE_CONFIG_HOME' environment variable.
 */
class XMLConfig {

    private static final String IGNITE_CONFIG_HOME = "IGNITE_CONFIG_HOME";

    static <T> T loadBean(String fileName, String beanName) {
        String location = System.getProperty(IGNITE_CONFIG_HOME);
        if (location == null) {
            location = System.getenv(IGNITE_CONFIG_HOME);
        }
        if (location == null) {
            throw new RuntimeException(String.format("Environment variable %s should be set and point to the folder containing %s.xml", IGNITE_CONFIG_HOME, fileName));
        }

        Path igniteConfigHomePath = Paths.get(location);
        Path beanPath = Paths.get(igniteConfigHomePath.toString(), fileName + ".xml");

        return Ignition.loadSpringBean(beanPath.toString(), beanName);
    }

}