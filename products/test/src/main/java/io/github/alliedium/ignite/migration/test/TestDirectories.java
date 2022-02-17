package io.github.alliedium.ignite.migration.test;

import java.nio.file.Path;
import java.nio.file.Paths;

public class TestDirectories {
    private final Path avroTestSetPath;
    private final Path avroMainPath;

    public TestDirectories() {
        avroTestSetPath = Paths.get("./avrotestset");
        avroMainPath = Paths.get("./avro");
    }

    public Path getAvroTestSetPath() {
        return avroTestSetPath;
    }

    public Path getAvroMainPath() {
        return avroMainPath;
    }
}
