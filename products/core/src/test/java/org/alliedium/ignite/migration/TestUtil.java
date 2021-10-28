package org.alliedium.ignite.migration;

import org.alliedium.ignite.migration.dao.converters.TypesResolver;
import org.alliedium.ignite.migration.serializer.utils.AvroFileExtensions;
import org.alliedium.ignite.migration.dto.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class TestUtil {
    private static final Logger logger = LoggerFactory.getLogger(AvroFileManagerTest.class);

    private static final String GENERIC_RECORD_KEY_FIELD_NAME = "key";
    private static final Random random = new Random();

    private static final String fieldName = "name";
    private static final String fieldType = TypesResolver.toAvroType(Integer.class.getName());

    public static ICacheData createTestCacheData(String cacheName) {
        List<ICacheEntryValueField> cacheEntryValueDTOFields = new ArrayList<>();
        CacheEntryValueField field = new CacheEntryValueField.Builder()
                .setName(GENERIC_RECORD_KEY_FIELD_NAME)
                .setTypeClassName(TypesResolver.toAvroType(String.class.getName()))
                .setValue(GENERIC_RECORD_KEY_FIELD_NAME)
                .build();
        cacheEntryValueDTOFields.add(field);

        CacheEntryValue key = new CacheEntryValue(cacheEntryValueDTOFields);
        cacheEntryValueDTOFields = new ArrayList<>();
        field = new CacheEntryValueField.Builder()
                .setName(fieldName)
                .setTypeClassName(fieldType)
                .setValue(random.nextInt())
                .build();
        cacheEntryValueDTOFields.add(field);
        CacheEntryValue val = new CacheEntryValue(cacheEntryValueDTOFields);

        return new CacheData(cacheName, key, val);
    }

    public static Map<String, String> getFieldsTypesForTestCacheData() {
        Map<String, String> fieldsTypes = new HashMap<>();
        fieldsTypes.put(fieldName, fieldType);
        return fieldsTypes;
    }

    public static Schema getSchemaForAvroFile(Path avroFilePath) throws IOException {
        Path avroFileName = avroFilePath.getFileName();
        String avroSchemaFileName = getAvroSchemaFileName(avroFileName.toString());
        Path avroSchemaFilePath = Paths.get(avroFilePath.getParent().toString(), avroSchemaFileName);
        return new Schema.Parser().parse(avroSchemaFilePath.toFile());
    }

    public static String getAvroSchemaFileName(String initialFileName) {
        return "schema_" + initialFileName.substring(0, initialFileName.length() - AvroFileExtensions.AVRO.length()) + AvroFileExtensions.AVSC;
    }

    public static boolean compareTwoFiles(File file1, File secondFile) throws IOException {
        BufferedReader reader1 = new BufferedReader(new FileReader(file1));
        BufferedReader reader2 = new BufferedReader(new FileReader(secondFile));

        String line1 = reader1.readLine();
        String line2 = reader2.readLine();
        boolean areEqual = true;
        int lineNum = 1;

        while (line1 != null || line2 != null) {
            if (line1 == null || line2 == null) {
                areEqual = false;
                break;
            }
            else if (!line1.equalsIgnoreCase(line2) && !file1.getPath().contains(AvroFileExtensions.AVSC)) {
                areEqual = false;
                break;
            }

            line1 = reader1.readLine();
            line2 = reader2.readLine();
            lineNum++;
        }

        if (!areEqual) {
            logger.warn(file1 + " file differs from " + secondFile + " file. Difference is at line " + lineNum);
        }

        reader1.close();
        reader2.close();

        return areEqual;
    }

    public static boolean compareDeserializedData(List<GenericRecord> firstFileDeserializedData, List<GenericRecord> secondFileDeserializedData) {
        boolean areEqual = true;
        if (firstFileDeserializedData.size() != secondFileDeserializedData.size()) {
            areEqual = false;
        }

        List<String> firstDataKeysList = getKeysListFromGenericRecordList(firstFileDeserializedData);
        List<String> secondDataKeysList = getKeysListFromGenericRecordList(secondFileDeserializedData);

        if (!areEqual) {
            areEqual = sortAndCompareLists(firstDataKeysList, secondDataKeysList);
        }

        if (!areEqual) {
            for (String key : firstDataKeysList) {
                Optional<GenericRecord> firstGenericRecord = getGenericRecordFromList(firstFileDeserializedData, key);
                Optional<GenericRecord >secondGenericRecord = getGenericRecordFromList(secondFileDeserializedData, key);
                if (!firstGenericRecord.equals(secondGenericRecord)) {
                    logger.info(
                            "Mismatch: generic record from first file not equals to generic record from second file. First record: " + firstGenericRecord + " ; second record: " + secondGenericRecord);
                    areEqual = false;
                }
            }
        }

        return areEqual;
    }

    private static Optional<GenericRecord> getGenericRecordFromList(List<GenericRecord> genericRecordList, String key) {
        return genericRecordList.stream()
                .filter(record -> record.get(GENERIC_RECORD_KEY_FIELD_NAME).equals(key))
                .findFirst();
    }

    private static List<String> getKeysListFromGenericRecordList(List<GenericRecord> genericRecordList) {
        return genericRecordList.stream()
                .map(record -> record.get(GENERIC_RECORD_KEY_FIELD_NAME).toString())
                .collect(Collectors.toList());
    }

    private static boolean sortAndCompareLists(List<String> firstList, List<String> secondList) {
        Collections.sort(firstList);
        Collections.sort(secondList);
        return firstList.equals(secondList);
    }
}
