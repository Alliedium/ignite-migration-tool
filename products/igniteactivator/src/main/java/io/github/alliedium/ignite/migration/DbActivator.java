package io.github.alliedium.ignite.migration;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.logging.Logger;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class DbActivator {

    protected static final Logger logger = Logger.getLogger(DbActivator.class.getName());
    private static final long ACTIVATION_CHECK_PERIODICITY_MILLIS = 1000L;
    private static final String IGNITE_CONFIG_HOME = "IGNITE_CONFIG_HOME";

    private static void waitTillInterrupted() {
        //noinspection InfiniteLoopStatement
        while (true) {
            try {
                print("Sleeping, press Ctrl-C for interruption...");
                //noinspection BusyWait
                Thread.sleep(Long.MAX_VALUE);
            }
            catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("n", "numnodes", true, "Expected number of baseline nodes");
        options.addOption("t", "timeout", true, "Maximal timeout for start of all nodes before activation in seconds");
        options.addOption("w", "wait", false, "Wait till interruption (do not exit)");
        options.addOption("h", "help", false, "Print this help");

        HelpFormatter helpFormatter = new HelpFormatter();
        CommandLine line = null;

        try {
            CommandLineParser parser = new DefaultParser();
            line = parser.parse(options, args);

        }
        catch (ParseException e) {
            print("Unexpected exception: " + e.getMessage());
            helpFormatter.printHelp("...", options);
            System.exit(-1);
        }

        if (line.hasOption("help")) {
            helpFormatter.printHelp("...", options);
            System.exit(0);
        }

        int numNodes = 0;
        long activationTimeout = 0L;

        try {
            numNodes = Integer.parseInt(line.getOptionValue("numnodes", "1"));
            if (numNodes <= 0) {
                throw new Exception("numnodes should be positive integer");
            }
        }
        catch (Exception e) {
            print("Wrong value of numnodes parameter: " + e.getMessage());
            System.exit(-1);
        }

        try {
            activationTimeout = Long.parseLong(line.getOptionValue("timeout", "300"));
            if (activationTimeout <= 0) {
                throw new Exception("timeout should be positive integer");
            }
        }
        catch (Exception e) {
            print("Wrong value of timeout parameter: " + e.getMessage());
            System.exit(-1);
        }
        activationTimeout *= 1000L;

        String location = System.getProperty(IGNITE_CONFIG_HOME);
        if (location == null) {
            location = System.getenv(IGNITE_CONFIG_HOME);
        }
        if (location == null) {
            throw new RuntimeException(String.format("Environment variable %s should be set and point to the folder containing client.xml", IGNITE_CONFIG_HOME));
        }

        Path configHomePath = Paths.get(location);
        Path beanPath = Paths.get(configHomePath.toString(), "client.xml");
        Ignite ignite = Ignition.start(beanPath.toString());
        String nodeStartupWaitMsg = String.format("Waiting until %d server nodes are started and active", numNodes);
        print(nodeStartupWaitMsg + "...");
        long startTime = System.currentTimeMillis();
        final Runnable niceExit = () -> {
            ignite.close();
            System.exit(-1);
        };
        while (true) {
            int serverNodes = ignite.cluster().forServers().nodes().size();
            print(String.format("%d server nodes are available", serverNodes));
            if (serverNodes >= numNodes) {
                break;
            }
            else {
                try {
                    //noinspection BusyWait
                    Thread.sleep(ACTIVATION_CHECK_PERIODICITY_MILLIS);
                }
                catch (InterruptedException ex) {
                    print(nodeStartupWaitMsg + " is interrupted");
                    niceExit.run();
                }
                if (System.currentTimeMillis() - startTime > activationTimeout) {
                    print(nodeStartupWaitMsg + ": timeout is exhausted");
                    niceExit.run();
                }
            }

        }
        print(nodeStartupWaitMsg + ": done");
        print("Activation of Ignite cluster...");
        ignite.cluster().active(true);
        print("Activation of Ignite cluster: done");

        ignite.close();
        print("Activator is now disconnected from Ignite cluster.");
        if (line.hasOption("wait")) {
            waitTillInterrupted();
        }
        else {
            System.exit(0);
        }
    }


    /**
     * Prints message.
     *
     * @param msg Message to print before all objects are printed.
     */
    private static void print(String msg) {
        logger.info(">>> " + msg);
    }
}