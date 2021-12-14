package org.alliedium.ignite.migration.patchtools;

import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.isolated.IsolatedDiscoverySpi;
import org.testng.annotations.AfterMethod;

import java.io.IOException;

public class IsolatedIgniteNodePerTest extends BaseTest {

    private IgniteConfiguration configuration;

    @Override
    protected IgniteConfiguration loadIgniteConfig() {
        IgniteConfiguration configuration = new IgniteConfiguration();
        configuration.setIgniteInstanceName(getClass().getName());
        configuration.setClientMode(false);
        configuration.setDiscoverySpi(new IsolatedDiscoverySpi());
        this.configuration = configuration;
        return configuration;
    }

    @Override
    @AfterMethod
    public void afterMethod() throws IOException {
        super.afterMethod();
        Ignition.getOrStart(configuration).close();
    }
}
