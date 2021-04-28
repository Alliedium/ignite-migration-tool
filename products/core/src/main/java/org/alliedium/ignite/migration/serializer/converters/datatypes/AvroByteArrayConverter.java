package org.alliedium.ignite.migration.serializer.converters.datatypes;

import java.util.Base64;
import org.apache.avro.util.Utf8;

/**
 * Converter required for avro-serialization of the byte-array values.
 * Values are being converted to String. Reverse conversion is also available.
 * Fields with converted byte-array value are also being correspondingly marked in avro schema.
 *
 */
public class AvroByteArrayConverter implements IAvroDerivedTypeConverter { //TODO: get rid of current converter. Byte arrays need to be serialized to avro using native avro functionalities.

    public String convertForAvro(Object fieldData) {
        assert fieldData instanceof byte[];
        return Base64.getEncoder().encodeToString((byte[]) fieldData);
    }

    public byte[] convertFromAvro(Object fieldData) {
        assert (fieldData instanceof String) || (fieldData instanceof Utf8);
        return Base64.getDecoder().decode(fieldData.toString());
    }

}
