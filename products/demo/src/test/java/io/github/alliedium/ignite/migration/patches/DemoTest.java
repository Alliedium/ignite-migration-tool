package io.github.alliedium.ignite.migration.patches;

import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.testng.Assert.*;

public class DemoTest extends TemporaryFolderTest {

    private static final String processNotFinishedYetMsg = "process not finished yet, you have to fix issues if they exist" +
            " otherwise you may want to increase the process waiting time";

    @Test
    public void testRunDemo() throws IOException, InterruptedException {

        File resultFile = createTempFile();

        Process process = executeScript("./run_demo.sh", resultFile);

        boolean finished = process.waitFor(4, TimeUnit.MINUTES);
        checkProcessFinished(process, finished);
        List<String> lines = Files.readAllLines(resultFile.toPath());
        assertEquals(lines, Stream.of(
                "Successfully serialized data into avro",
                "Successfully applied patches to data",
                "--- checked that field [age] was added successfully into cache [first] ---",
                "--- checked that field [population] was removed successfully from cache [second] ---",
                "Successfully loaded data from avro into ignite and checked patches applied correctly"
        ).collect(Collectors.toList()));
    }

    @Test
    public void testRunDemoNested() throws IOException, InterruptedException {
        File resultFile = createTempFile();

        Process process = executeScript("./run_demo_nested.sh", resultFile);
        boolean finished = process.waitFor(4, TimeUnit.MINUTES);
        checkProcessFinished(process, finished);

        List<String> lines = Files.readAllLines(resultFile.toPath());
        assertEquals(lines, Stream.of(
                "Successfully serialized data into avro",
                "Successfully applied patches to data",
                "Successfully loaded data from avro into ignite and checked patches applied correctly"
        ).collect(Collectors.toList()));
    }

    @Test
    public void testRunDemoJoin() throws IOException, InterruptedException {
        File resultFile = createTempFile();

        Process process = executeScript("./run_demo_join.sh", resultFile);
        boolean finished = process.waitFor(4, TimeUnit.MINUTES);
        checkProcessFinished(process, finished);

        List<String> lines = Files.readAllLines(resultFile.toPath());
        assertEquals(lines, Stream.of(
                "Successfully serialized data into avro",
                "Successfully applied patches to data",
                "Successfully loaded data from avro into ignite and checked patches applied correctly"
        ).collect(Collectors.toList()));
    }

    /**
     * This is a test for debug purpose.
     * Actions to be done in order to debug and explore:
     *  1. Start ignite and igniteactivator in docker, use the next command:
     *      docker-compose up --build ignite igniteactivator
     *  2. Uncomment the @Test annotation of this test
     *  3. Put break points where it is needed and run the test in debug mode from your IDE
     */
    //@Test
    public void tesForDebug() throws IOException {
        new CreateDataForJoin(new String[] {"./avro_original"}).execute();
        new MakeJoin(new String[] {"./avro_original", "./avro_transformed"}).execute();
        new CheckJoin(new String[] {"./avro_transformed"}).execute();
    }

    public Process executeScript(String command, File file) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command(command, file.toString());
        processBuilder.redirectErrorStream(true);
        processBuilder.inheritIO();

        return processBuilder.start();
    }

    public void executeCommand(String command) throws IOException, InterruptedException {
        ProcessBuilder processBuilder = new ProcessBuilder();
        processBuilder.command("sh", "-c", command);
        processBuilder.redirectErrorStream(true);
        processBuilder.inheritIO();
        Process process = null;
        try {
            process = processBuilder.start();
            process.waitFor();
        } finally {
            if (process != null) {
                process.destroy();
            }
        }
    }

    public void checkProcessFinished(Process process, boolean processFinished) throws IOException, InterruptedException {
        try {
            assertTrue(processFinished, processNotFinishedYetMsg);
        } finally {
            try {
                executeCommand("ps ax | grep \"demo.*jar-with-dependencies\\.jar\" | awk {'print $1'} | xargs kill");
                executeCommand("docker-compose down");
            } finally {
                process.destroyForcibly();
            }
        }
    }
}