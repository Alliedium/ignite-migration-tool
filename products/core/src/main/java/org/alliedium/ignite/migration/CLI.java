package org.alliedium.ignite.migration;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.alliedium.ignite.migration.properties.AtomicLongNamesProviderFactory;
import org.alliedium.ignite.migration.properties.PropertyNames;
import org.alliedium.ignite.migration.properties.PropertiesResolver;
import org.apache.commons.cli.*;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.bridge.SLF4JBridgeHandler;

/**
 * An arguments-based Command-line interface.
 */
public class CLI {

    private static final Logger logger = LoggerFactory.getLogger(CLI.class);
    private static Path serializedDataStoragePath = Paths.get("avro");

    public static void main(String[] args) throws IOException {
        main(args, true);
    }

    public static void main(String[] args, boolean closeIgniteAfter) throws IOException {
        SLF4JBridgeHandler.removeHandlersForRootLogger();
        SLF4JBridgeHandler.install();

        Options options = new Options();
        options.addOption("s", PropertyNames.CLI.SERIALIZE, false, "Serialize Apache Ignite cache stored data, save serialized data on disk");
        options.addOption("d", PropertyNames.CLI.DESERIALIZE, false, "Deserialize disk stored data, restore Apache Ignite caches, upload deserialized data into restored caches");
        options.addOption("p", PropertyNames.CLI.PATH, true, "Path for data serialization/deserialization (default is 'avro')");
        options.addOption("h", PropertyNames.CLI.HELP, false, "Print this help");

        HelpFormatter helpFormatter = new HelpFormatter();
        CommandLine line = null;

        try {
            CommandLineParser parser = new DefaultParser();
            line = parser.parse(options, args);
        }
        catch (ParseException e) {
            logger.info("Unexpected exception: " + e.getMessage());
            helpFormatter.printHelp("...", options);
            java.lang.System.exit(-1);
        }

        if ((line.hasOption(PropertyNames.CLI.SERIALIZE) && line.hasOption(PropertyNames.CLI.DESERIALIZE))
                || (!line.hasOption(PropertyNames.CLI.SERIALIZE) && !line.hasOption(PropertyNames.CLI.DESERIALIZE)) && !line.hasOption(PropertyNames.CLI.HELP)) {
            logger.info(String.format("Either '--%s' or '--%s' argument is expected (using both is prohibited)",
                    PropertyNames.CLI.SERIALIZE, PropertyNames.CLI.DESERIALIZE));
            helpFormatter.printHelp("...", options);
            java.lang.System.exit(-1);
        }

        if (line.hasOption(PropertyNames.CLI.HELP)) {
            helpFormatter.printHelp("...", options);
            java.lang.System.exit(0);
        }

        if (line.hasOption(PropertyNames.CLI.PATH)) {
            try {
                serializedDataStoragePath = Paths.get(line.getOptionValue(PropertyNames.CLI.PATH));
            }
            catch (Exception e) {
                logger.info("Provided path is either incorrect or nonexistent: " + e.getMessage());
                java.lang.System.exit(-1);
            }
        }

        PropertiesResolver propertiesResolver = loadToolPropertiesResolver();

        IgniteConfiguration igniteConfiguration = IgniteConfigLoader.load("client");
        Ignite ignite = Ignition.getOrStart(igniteConfiguration);

        IgniteAtomicLongNamesProvider atomicLongNamesProvider =
                new AtomicLongNamesProviderFactory(ignite).create(propertiesResolver);
        Controller controller = new Controller(ignite, atomicLongNamesProvider, propertiesResolver);

        if (line.hasOption(PropertyNames.CLI.SERIALIZE)) {
            logger.info("Ignite migration tool was started with '--serialize' argument. The following path will be used for avro files storing: " + "\"" + serializedDataStoragePath + "\"");
            controller.serializeDataToAvro(serializedDataStoragePath);
        }
        if (line.hasOption(PropertyNames.CLI.DESERIALIZE)) {
            logger.info("Ignite migration tool was started with '--deserialize' argument. The following path will be used for avro files picking: " + "\"" + serializedDataStoragePath + "\"");
            controller.deserializeDataFromAvro(serializedDataStoragePath);
        }

        if (closeIgniteAfter) {
            ignite.close();
        }
    }

    private static PropertiesResolver loadToolPropertiesResolver() throws IOException {
        if (System.getProperty(PropertyNames.System.PROPERTIES_FILE_PATH) == null) {
            return PropertiesResolver.loadProperties();
        }

        return PropertiesResolver.loadProperties(System.getProperty(PropertyNames.System.PROPERTIES_FILE_PATH));
    }
}
