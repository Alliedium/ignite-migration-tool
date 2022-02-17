package io.github.alliedium.ignite.migration.properties;

public interface PropertyNames {
    interface System {
        String PROPERTIES_FILE_PATH = "properties.file.path";
    }
    interface CLI {
        String SERIALIZE = "serialize";
        String DESERIALIZE = "deserialize";
        String PATH = "path";
        String HELP = "help";
    }

    String ATOMIC_LONG_NAMES_PROPERTY = "ignite.atomic.long.names";
    String DISPATCHERS_ELEMENTS_LIMIT = "dispatchers.elements.limit";
    String ATOMIC_LONG_NAMES_CLASS_PROVIDER = "ignite.atomic.long.names.provider";
}
