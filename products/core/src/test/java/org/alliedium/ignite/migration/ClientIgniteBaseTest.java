package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.test.ClientAPI;
import org.apache.ignite.Ignite;
import org.junit.Before;
import org.junit.BeforeClass;

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
    public static void beforeClass() {
        clientAPI = ClientAPI.loadClientIgnite(IgniteConfigLoader.load("client"));
        avroMainPath = clientAPI.getAvroMainPath();
        avroTestSet = clientAPI.getAvroTestSetPath();
        ignite = clientAPI.getIgnite();
        random = clientAPI.getRandom();
    }

    @Before
    public void before() throws IOException {
        clientAPI.cleanIgniteAndRemoveDirectories();
    }
}