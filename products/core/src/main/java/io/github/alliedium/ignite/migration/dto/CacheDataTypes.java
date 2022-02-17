package io.github.alliedium.ignite.migration.dto;

public class CacheDataTypes {
    private final String keyType;
    private final String valType;

    public CacheDataTypes(String keyType, String valType) {
        this.keyType = keyType;
        this.valType = valType;
    }

    public String getKeyType() {
        return keyType;
    }

    public String getValType() {
        return valType;
    }
}
