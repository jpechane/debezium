/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.data;

import java.util.List;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import io.debezium.util.Strings;

/**
 * A semantic type for an enumeration, where the string values are one of the enumeration's values.
 * 
 * @author Randall Hauch
 */
public class Enum {

    public static final String LOGICAL_NAME = "io.debezium.data.Enum";
    public static final String VALUES_FIELD = "allowed";
    public static final String AVRO_CONVERTER_TYPE_ENUM = "io.confluent.connect.avro.Enum";

    /**
     * Returns a {@link SchemaBuilder} for an enumeration. You can use the resulting SchemaBuilder
     * to set additional schema settings such as required/optional, default value, and documentation.
     * 
     * @param allowedValues the list of allowed values; may not be null
     * @param typeName name of the enum type
     * @param avroCompatible true if encoding supported by Avro converter should be used,
     * false if Debezium standard encoding should be used
     * @return the schema builder
     */
    public static SchemaBuilder builder(List<String> allowedValues, String typeName, boolean avroCompatible) {
        if (avroCompatible) {
            final SchemaBuilder builder = SchemaBuilder.string()
                                                       .name(LOGICAL_NAME)
                                                       .parameter(AVRO_CONVERTER_TYPE_ENUM, typeName)
                                                       .version(1);
            allowedValues.forEach(x -> builder.parameter(AVRO_CONVERTER_TYPE_ENUM + "." + x, x));
            return builder;
        }
        else {
            return SchemaBuilder.string()
                                .name(LOGICAL_NAME)
                                .parameter(VALUES_FIELD, Strings.join(",", allowedValues))
                                .version(1);
        }
    }

    /**
     * Returns a {@link SchemaBuilder} for an enumeration, with all other default Schema settings.
     * 
     * @param allowedValues the list of allowed values; may not be null
     * @param typeName name of the enum type
     * @param avroCompatible true if encoding supported by Avro converter should be used,
     * false if Debezium standard encoding should be used
     * @return the schema
     * @see #builder(String)
     */
    public static Schema schema(List<String> allowedValues, String typeName, boolean avroCompatible) {
        return builder(allowedValues, typeName, avroCompatible).build();
    }
}
