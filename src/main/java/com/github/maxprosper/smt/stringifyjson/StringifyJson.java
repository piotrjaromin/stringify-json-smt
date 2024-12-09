package com.github.maxprosper.smt.stringifyjson;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Values;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import org.json.JSONObject;
import org.json.JSONArray;
import org.json.JSONException;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

/**
 * Main class that implements stringify JSON transformation.
 */
@SuppressWarnings("unchecked")
public class StringifyJson<R extends ConnectRecord<R>> implements Transformation<R> {
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(StringifyJson.class);

    private static final String PURPOSE = "StringifyJson SMT";
    private List<String> targetFields;

    private final String delimiterJoin = ".";

    interface ConfigName {
        String TARGET_FIELDS = "targetFields";
    }

    private static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(ConfigName.TARGET_FIELDS,
                    ConfigDef.Type.LIST, "",
                    ConfigDef.Importance.HIGH,
                    "Names of target fields. These fields will be stringified.");

    @Override
    public void configure(Map<String, ?> configs) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, configs);
        targetFields = config.getList(ConfigName.TARGET_FIELDS);
    }

    @Override
    public R apply(R record) {
        Map<String, Object> value = (Map<String, Object>) record.value();
        if (value == null) {
            LOGGER.info("Record is null");
            LOGGER.info(record.toString());
            return record;
        }

        for (String field : targetFields) { 
            if (value.containsKey(field)) {
                try {
                    String eventsString = objectMapper.writeValueAsString(value.get(field));
                    value.put(field, eventsString);
                } catch (Exception e) { 
                    throw new RuntimeException("Failed to serialize 'events' field to string", e);
                }
            } else {
                LOGGER.info("Stringify - Field does not exist");
            }
        }

        return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(),
                record.valueSchema(), value, record.timestamp());
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    @Override
    public void close() {
    }
}
