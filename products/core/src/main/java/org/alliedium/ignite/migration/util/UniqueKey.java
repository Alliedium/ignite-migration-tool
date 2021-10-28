package org.alliedium.ignite.migration.util;

import java.util.Optional;
import java.util.UUID;

public class UniqueKey {

    private static final String recordTypeIdentifier = "_recordType_";

    public static String generate() {
        return "u_" + UUID.randomUUID().toString().replace("-", "_");
    }

    public static String generateWithRecordType(String type) {
       return  UniqueKey.generate() + recordTypeIdentifier + type;
    }

    public static Optional<String> getRecordType(String uniqueKey) {
        int index = uniqueKey.indexOf(recordTypeIdentifier);
        if (index == -1) {
            return Optional.empty();
        }

        String recordType = uniqueKey.substring(index + recordTypeIdentifier.length());
        return Optional.of(recordType);
    }
}
