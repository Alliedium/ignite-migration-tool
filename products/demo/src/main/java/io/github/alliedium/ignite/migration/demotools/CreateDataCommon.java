package io.github.alliedium.ignite.migration.demotools;

import io.github.alliedium.ignite.migration.Controller;
import io.github.alliedium.ignite.migration.IgniteAtomicLongNamesProvider;
import io.github.alliedium.ignite.migration.IgniteConfigLoader;
import io.github.alliedium.ignite.migration.test.ClientAPI;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class CreateDataCommon {

    protected final ClientAPI clientAPI;
    protected final Path pathToSerialize;
    protected final ResultFileWriter resultWriter;

    public CreateDataCommon(String[] args) {
        clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        pathToSerialize = args.length > 0 ? Paths.get(args[0]) : clientAPI.getAvroTestSetPath();
        resultWriter = new ResultFileWriter(args, 1);
    }

    protected void destroyPreviousData() throws IOException {
        clientAPI.cleanIgniteAndRemoveDirectories();
        ClientAPI.deleteDirectoryRecursively(pathToSerialize);
        ClientAPI.deleteDirectoryRecursively(clientAPI.getAvroMainPath());
    }

    public void execute() throws IOException {
        destroyPreviousData();

        createData();

        writeIgniteDataIntoAvro();

        writeSuccess();
    }

    protected void writeSuccess() {
        resultWriter.writeLine("Successfully serialized data into avro");
    }

    protected void writeIgniteDataIntoAvro() {
        Controller controller = new Controller(clientAPI.getNotMockedIgnite(), IgniteAtomicLongNamesProvider.EMPTY);
        controller.serializeDataToAvro(pathToSerialize);
    }

    protected abstract void createData();
}
