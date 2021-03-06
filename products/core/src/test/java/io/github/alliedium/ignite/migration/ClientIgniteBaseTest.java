package io.github.alliedium.ignite.migration;

import io.github.alliedium.ignite.migration.test.ClientAPI;
import org.apache.ignite.Ignite;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;

import java.io.IOException;
import java.nio.file.Path;
import java.util.*;

public class ClientIgniteBaseTest {

    protected static ClientAPI clientAPI;
    protected static Path avroTestSet;
    protected static Path avroMainPath;
    protected static Ignite ignite;
    protected static Random random;

    @BeforeClass
    public void beforeClass() {
        clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        avroMainPath = clientAPI.getAvroMainPath();
        avroTestSet = clientAPI.getAvroTestSetPath();
        ignite = clientAPI.getIgnite();
        random = clientAPI.getRandom();
    }

    @BeforeMethod
    public void before() throws IOException {
        clientAPI.cleanIgniteAndRemoveDirectories();
    }
}
