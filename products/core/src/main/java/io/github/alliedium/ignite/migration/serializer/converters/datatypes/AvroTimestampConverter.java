package io.github.alliedium.ignite.migration.serializer.converters.datatypes;

import java.sql.Timestamp;

/**
 * Converter required for avro-serialization of the {@link Timestamp} values, which are not supported in avro.
 * Values are being converted to 'Long'. Reverse conversion is also available.
 * Fields with converted {@link Timestamp} value are also being correspondingly marked in avro schema.
 *
 */
class AvroTimestampConverter implements IAvroDerivedTypeConverter {

    public Long convertForAvro(Object fieldData) {
        assert fieldData instanceof Timestamp;
        Timestamp valueAsTimestamp = (Timestamp) fieldData;
        return valueAsTimestamp.getTime();
    }

    public Timestamp convertFromAvro(Object fieldData) {
        assert fieldData instanceof Long;
        return new Timestamp((long) fieldData);
    }

}
