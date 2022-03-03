package io.github.alliedium.ignite.migration.patches;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class TemporaryFolderTest {

    private static final String TMP_PREFIX = "shellTests";
    private File tempDir;

    @BeforeMethod
    public void before() {
        tempDir = createTempDirectory();
    }

    @AfterMethod
    public void after() {
        recursiveDelete(tempDir);
    }

    private File createTempDirectory() {
        try {
            return Files.createTempDirectory(TMP_PREFIX).toFile();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected File createTempFile() throws IOException {
        return Files.createTempFile(tempDir.toPath(), null, null).toFile();
    }

    private static boolean recursiveDelete(File file) {
        // Try deleting file before assuming file is a directory
        // to prevent following symbolic links.
        if (file.delete()) {
            return true;
        }
        File[] files = file.listFiles();
        if (files != null) {
            for (File each : files) {
                if (!recursiveDelete(each)) {
                    return false;
                }
            }
        }
        return file.delete();
    }
}
