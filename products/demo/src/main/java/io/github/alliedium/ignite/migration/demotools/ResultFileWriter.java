package io.github.alliedium.ignite.migration.demotools;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Optional;

public class ResultFileWriter {

    private final Optional<Path> resultFilePath;

    public ResultFileWriter(String[] args, int index) {
        Path path = null;
        if (args.length > index) {
            path = Paths.get(args[index]);
        }
        this.resultFilePath = Optional.ofNullable(path);
    }

    public void writeLine(String line) {
        resultFilePath.ifPresent(path -> {
            byte[] resultBytes = (line + "\n").getBytes(StandardCharsets.UTF_8);
            try {
                Files.write(path, resultBytes, StandardOpenOption.APPEND);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
