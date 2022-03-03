package io.github.alliedium.ignite.migration.demotools;

import io.github.alliedium.ignite.migration.Controller;
import io.github.alliedium.ignite.migration.IgniteAtomicLongNamesProvider;
import io.github.alliedium.ignite.migration.IgniteConfigLoader;
import io.github.alliedium.ignite.migration.test.ClientAPI;

import java.nio.file.Path;
import java.nio.file.Paths;

public abstract class LoadAndCheckDataCommon {

    protected final ClientAPI clientAPI;
    protected final ResultFileWriter resultWriter;
    protected final Path serializedDataPath;

    public LoadAndCheckDataCommon(String[] args) {
        clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        serializedDataPath = args.length > 0 ? Paths.get(args[0]) : clientAPI.getAvroMainPath();
        resultWriter = new ResultFileWriter(args, 1);

        clientAPI.clearIgniteAndCheckIgniteIsEmpty();
    }

    protected void readDataFromAvro() {
        Controller controller = new Controller(clientAPI.getIgnite(), IgniteAtomicLongNamesProvider.EMPTY);
        controller.deserializeDataFromAvro(serializedDataPath);
    }

    public void execute() {
        readDataFromAvro();

        checkData();

        clientAPI.getNotMockedIgnite().close();

        writeSuccess();
    }

    protected abstract void checkData();

    protected void writeSuccess() {
        resultWriter.writeLine("Successfully loaded data from avro into ignite and checked patches applied correctly");
    }
}
