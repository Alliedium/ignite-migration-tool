package io.github.alliedium.ignite.migration.util;

import java.util.UUID;

public class UniqueKey {

    public static String generate() {
        return "u_" + UUID.randomUUID().toString().replace("-", "_");
    }
}
