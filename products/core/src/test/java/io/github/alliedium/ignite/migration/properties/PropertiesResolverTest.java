package io.github.alliedium.ignite.migration.properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;

public class PropertiesResolverTest {

    @Test
    public void testAtomicsNameProviderGetsResolved() {
        String className = "className";
        Properties properties = mock(Properties.class);
        when(properties.getProperty(PropertyNames.ATOMIC_LONG_NAMES_CLASS_PROVIDER)).thenReturn(className);
        PropertiesResolver resolver = new PropertiesResolver(properties);
        Assert.assertEquals(className, resolver.getIgniteAtomicLongNamesProviderClass().get());
    }
}