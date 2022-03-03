package io.github.alliedium.ignite.migration.demotools;

import io.github.alliedium.ignite.migration.patchtools.PatchContext;
import io.github.alliedium.ignite.migration.test.ClientAPI;
import io.github.alliedium.ignite.migration.test.TestDirectories;
import io.github.alliedium.ignite.migration.util.PathCombine;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class AlterDataCommon {

    protected final PathCombine source;
    protected final PathCombine destination;
    protected final ResultFileWriter resultWriter;

    protected PatchContext context;

    public AlterDataCommon(String[] args) {
        // resolve source and destination folders
        TestDirectories testDirectories = new TestDirectories();
        Path sourcePath = args.length > 1 ? Paths.get(args[0]) : testDirectories.getAvroTestSetPath();
        Path destinationPath = args.length > 1 ? Paths.get(args[1]) : testDirectories.getAvroMainPath();
        resultWriter = new ResultFileWriter(args, 2);
        source = new PathCombine(sourcePath);
        destination = new PathCombine(destinationPath);
    }

    protected void preparePatchContext() throws IOException {
        ClientAPI.deleteDirectoryRecursively(destination.getPath());
        context = new PatchContext(source, destination);
        context.prepare();
    }

    public void execute() throws IOException {
        preparePatchContext();

        alterData();

        writeSuccess();
    }

    protected void writeSuccess() {
        resultWriter.writeLine("Successfully applied patches to data");
    }

    protected abstract void alterData();
}
