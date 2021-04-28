package org.alliedium.ignite.migration;

import org.apache.ignite.configuration.IgniteConfiguration;

/**
 * Current class is responsible for getting the Ignite configuration details from the provided file.
 * Configuration file location is defined by 'IGNITE_CONFIG_HOME' environment variable.
 * Configurations are being checked to be not null, processed by {@link XMLConfig} class and then returned by current class as a correspondent object of IgniteConfiguration class.
 */
public class IgniteConfigLoader {

    private static final String igniteConfigBean = "default-ignite.cfg";

    public static IgniteConfiguration load(String fileName) throws RuntimeException {
        return XMLConfig.loadBean(fileName, igniteConfigBean);
    }
}