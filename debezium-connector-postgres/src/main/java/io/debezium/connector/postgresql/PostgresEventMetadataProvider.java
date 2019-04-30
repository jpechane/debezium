/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.postgresql;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import org.apache.kafka.connect.data.Struct;

import io.debezium.data.Envelope;
import io.debezium.pipeline.source.spi.EventMetadataProvider;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.schema.DataCollectionId;
import io.debezium.time.Conversions;
import io.debezium.util.Collect;

class PostgresEventMetadataProvider implements EventMetadataProvider {

    @Override
    public Instant getEventTimestamp(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        final Long timestamp = sourceInfo.getInt64(SourceInfo.TIMESTAMP_KEY);
        return timestamp == null ? null : Conversions.toInstantFromMicros(timestamp);
    }

    @Override
    public Map<String, String> getEventSourcePosition(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        // TODO Add metadata
        return Collections.emptyMap();
    }

    @Override
    public String getTransactionId(DataCollectionId source, OffsetContext offset, Object key, Struct value) {
        if (value == null) {
            return null;
        }
        final Struct sourceInfo = value.getStruct(Envelope.FieldName.SOURCE);
        if (source == null) {
            return null;
        }
        return Long.toString(sourceInfo.getInt64(SourceInfo.TXID_KEY));
    }
}
