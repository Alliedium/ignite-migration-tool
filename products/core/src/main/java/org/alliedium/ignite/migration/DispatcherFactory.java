package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.properties.PropertiesResolver;

public class DispatcherFactory {

    private PropertiesResolver propertiesResolver;

    public DispatcherFactory(PropertiesResolver propertiesResolver) {
        this.propertiesResolver = propertiesResolver;
    }

    public <T> Dispatcher<T> newDispatcher() {
        return new Dispatcher<>(propertiesResolver.getDispatchersElementsLimit());
    }
}
