package org.alliedium.ignite.migration.properties;

import org.alliedium.ignite.migration.IgniteAtomicLongNamesProvider;
import org.alliedium.ignite.migration.test.DefaultIgniteAtomicLongNamesProvider;
import org.apache.ignite.Ignite;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AtomicLongNamesProviderFactoryTest {

    private static final List<String> defaultAtomicNames = Stream.of("hello", "atomic")
            .collect(Collectors.toList());

    @Test
    public void testProviderCreated() {
        PropertiesResolver resolver = mock(PropertiesResolver.class);
        when(resolver.getIgniteAtomicLongNamesProviderClass())
                .thenReturn(Optional.of(Provider.class.getName()));
        IgniteAtomicLongNamesProvider provider = new AtomicLongNamesProviderFactory(mock(Ignite.class)).create(resolver);
        Assert.assertEquals(defaultAtomicNames, provider.getAtomicNames());
    }

    @Test
    public void testAtomicNamesProvided() {
        PropertiesResolver resolver = mock(PropertiesResolver.class);
        when(resolver.getAtomicLongNames()).thenReturn(defaultAtomicNames);
        IgniteAtomicLongNamesProvider provider = new AtomicLongNamesProviderFactory(mock(Ignite.class)).create(resolver);
        Assert.assertEquals(defaultAtomicNames, provider.getAtomicNames());
    }

    @Test
    public void testNoAtomicNamesNoProviderClassProvided() {
        PropertiesResolver resolver = mock(PropertiesResolver.class);
        IgniteAtomicLongNamesProvider provider = new AtomicLongNamesProviderFactory(mock(Ignite.class)).create(resolver);
        Assert.assertTrue(provider.getAtomicNames().isEmpty());
    }

    @Test
    public void testProviderCreatedAndIgniteInjected() {
        PropertiesResolver resolver = mock(PropertiesResolver.class);
        Ignite ignite = mock(Ignite.class);
        when(ignite.cacheNames()).thenReturn(Stream.of("firstCache", "secondCache").collect(Collectors.toSet()));
        when(resolver.getIgniteAtomicLongNamesProviderClass())
                .thenReturn(Optional.of(DefaultIgniteAtomicLongNamesProvider.class.getName()));

        IgniteAtomicLongNamesProvider provider = new AtomicLongNamesProviderFactory(ignite).create(resolver);
        List<String> expected = Stream.of(
                "PROD_firstCache_seq",
                "firstCache_seq",
                "PROD_secondCache_seq",
                "secondCache_seq"
        ).collect(Collectors.toList());

        List<String> result = provider.getAtomicNames();

        Collections.sort(expected);
        Collections.sort(result);
        Assert.assertEquals(expected, result);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testBadPathProviderClassDoesNotImplementInterface() {
        PropertiesResolver resolver = mock(PropertiesResolver.class);
        when(resolver.getIgniteAtomicLongNamesProviderClass())
                .thenReturn(Optional.of(NotProvider.class.getName()));
        new AtomicLongNamesProviderFactory(mock(Ignite.class)).create(resolver);
    }

    @Test(expectedExceptions = IllegalStateException.class)
    public void testBadPathNoAccessibleProviderConstructors() {
        PropertiesResolver resolver = mock(PropertiesResolver.class);
        when(resolver.getIgniteAtomicLongNamesProviderClass())
                .thenReturn(Optional.of(NoAccessibleConstructorProvider.class.getName()));
        new AtomicLongNamesProviderFactory(mock(Ignite.class)).create(resolver);
    }

    public static class Provider implements IgniteAtomicLongNamesProvider {
        @Override
        public List<String> getAtomicNames() {
            return defaultAtomicNames;
        }
    }

    public static class NoAccessibleConstructorProvider extends Provider {
        private NoAccessibleConstructorProvider() {}
    }

    public static class NotProvider {
        public List<String> getAtomicNames() {
            return defaultAtomicNames;
        }
    }
}