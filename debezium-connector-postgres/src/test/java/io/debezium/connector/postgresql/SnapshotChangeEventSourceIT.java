/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */

package io.debezium.connector.postgresql;

import static io.debezium.connector.postgresql.TestHelper.PK_FIELD;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.source.SourceRecord;
import org.fest.assertions.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;

import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.postgresql.AbstractRecordsProducerTest.SchemaAndValueField;
import io.debezium.connector.postgresql.AbstractRecordsProducerTest.TestConsumer;
import io.debezium.connector.postgresql.junit.SkipTestDependingOnDatabaseVersionRule;
import io.debezium.data.Envelope;
import io.debezium.data.VerifyRecord;
import io.debezium.doc.FixFor;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.spi.ChangeEventSource.ChangeEventSourceContext;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.LoggingContext;

/**
 * Integration test for {@link SnapshotChangeEventSourceIT}
 *
 * @author Horia Chiorean (hchiorea@redhat.com)
 */
public class SnapshotChangeEventSourceIT extends AbstractRecordsProducerTest {

    @Rule
    public final TestRule skip = new SkipTestDependingOnDatabaseVersionRule();

    private PostgresSnapshotChangeEventSource eventSource;
    private PostgresTaskContext context;
    private ChangeEventQueue<DataChangeEvent> eventQueue;

    @Before
    public void before() throws Exception {
        TestHelper.dropAllSchemas();
        TestHelper.executeDDL("init_postgis.ddl");
        TestHelper.executeDDL("postgres_create_tables.ddl");
        TestHelper.executeDDL("postgis_create_tables.ddl");

        PostgresConnectorConfig config = new PostgresConnectorConfig(TestHelper.defaultConfig()
                .build());
        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );
        eventQueue = new ChangeEventQueue.Builder<DataChangeEvent>().maxQueueSize(10000).maxBatchSize(10000).pollInterval(Duration.ofMillis(100)).loggingContextSupplier(() -> LoggingContext.forConnector("a", "b", "c")).build();
    }

    @After
    public void after() throws Exception {
//        if (snapshotProducer != null) {
//            snapshotProducer.stop();
//        }
    }

    @Test
    @FixFor("DBZ-859")
    public void shouldGenerateSnapshotAndSendHeartBeat() throws Exception {
        // PostGIS must not be used
        TestHelper.dropAllSchemas();
        TestHelper.execute("CREATE TABLE t1 (pk SERIAL, aa integer, PRIMARY KEY(pk)); INSERT INTO t1 VALUES (default, 11)");

        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                    .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.INITIAL)
                    .with(PostgresConnectorConfig.INCLUDE_SCHEMA_CHANGES, true)
                    .with(Heartbeat.HEARTBEAT_INTERVAL, 300_000)
                    .build()
        );
        TopicSelector<TableId> selector = PostgresTopicSelector.create(config);
        context = new PostgresTaskContext(
                config,
                TestHelper.getSchema(config),
                selector
        );

        final PostgresSchema schema = TestHelper.getSchema(config);
        eventSource = new PostgresSnapshotChangeEventSource(
                config,
                null, //new PostgresOffsetContext("xx", "yy", null, null, null, false, false),
                TestHelper.create(),
                schema,
                new EventDispatcher<TableId>(config, selector, schema, eventQueue, config.getTableFilters().dataCollectionFilter(), DataChangeEvent::new),
                Clock.SYSTEM,
                new SnapshotProgressListener() {
                    
                    @Override
                    public void tableSnapshotCompleted(TableId tableId, long numRows) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void snapshotStarted() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void snapshotCompleted() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void snapshotAborted() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void rowsScanned(TableId tableId, long numRows) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void monitoredTablesDetermined(Iterable<TableId> tableIds) {
                        // TODO Auto-generated method stub
                        
                    }
                }
        );
        eventSource.execute(new ChangeEventSourceContext() {
            
            @Override
            public boolean isRunning() {
                // TODO Auto-generated method stub
                return true;
            }
        });
        TestConsumer consumer = testConsumer2(2);
//        snapshotProducer.start(consumer, e -> {});
//
        // Make sure we get the table schema record and heartbeat record
        consumer.await(TestHelper.waitTimeForRecords(), TimeUnit.SECONDS);

        final SourceRecord first = consumer.remove();
        VerifyRecord.isValidRead(first, PK_FIELD, 1);
        System.out.println(first);
        assertRecordOffsetAndSnapshotSource(first, true, true);
        final SourceRecord second = consumer.remove();
        Assertions.assertThat(second.topic()).startsWith("__debezium-heartbeat");
        assertRecordOffsetAndSnapshotSource(second, false, false);
    }

    @Test
    public void shouldGenerateSnapshotsForDefaultDatatypes() throws Exception {
        PostgresConnectorConfig config = new PostgresConnectorConfig(
                TestHelper.defaultConfig()
                    .build()
        );
        final PostgresSchema schema = TestHelper.getSchema(config);
        eventSource = new PostgresSnapshotChangeEventSource(
                config,
                null, //new PostgresOffsetContext("xx", "yy", null, null, null, false, false),
                TestHelper.create(),
                schema,
                new EventDispatcher<TableId>(config, PostgresTopicSelector.create(config), schema, eventQueue, config.getTableFilters().dataCollectionFilter(), DataChangeEvent::new),
                Clock.SYSTEM,
                new SnapshotProgressListener() {
                    
                    @Override
                    public void tableSnapshotCompleted(TableId tableId, long numRows) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void snapshotStarted() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void snapshotCompleted() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void snapshotAborted() {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void rowsScanned(TableId tableId, long numRows) {
                        // TODO Auto-generated method stub
                        
                    }
                    
                    @Override
                    public void monitoredTablesDetermined(Iterable<TableId> tableIds) {
                        // TODO Auto-generated method stub
                        
                    }
                }
        );
        eventSource.execute(new ChangeEventSourceContext() {
            
            @Override
            public boolean isRunning() {
                // TODO Auto-generated method stub
                return true;
            }
        });

        TestConsumer consumer = testConsumer2(ALL_STMTS.size(), "public", "Quoted__");

        //insert data for each of different supported types
        String statementsBuilder = ALL_STMTS.stream().collect(Collectors.joining(";" + System.lineSeparator())) + ";";
        TestHelper.execute(statementsBuilder);

        //then start the producer and validate all records are there
        consumer.await(TestHelper.waitTimeForRecords() * 30, TimeUnit.SECONDS);

        Map<String, List<SchemaAndValueField>> expectedValuesByTopicName = super.schemaAndValuesByTopicName();
        consumer.process(record -> assertReadRecord(record, expectedValuesByTopicName));

        // check the offset information for each record
        while (!consumer.isEmpty()) {
            SourceRecord record = consumer.remove();
            assertRecordOffsetAndSnapshotSource(record, true, consumer.isEmpty());
            assertSourceInfo(record);
        }
    }

    private void assertReadRecord(SourceRecord record, Map<String, List<SchemaAndValueField>> expectedValuesByTopicName) {
        VerifyRecord.isValidRead(record, PK_FIELD, 1);
        String topicName = record.topic().replace(TestHelper.TEST_SERVER + ".", "");
        List<SchemaAndValueField> expectedValuesAndSchemasForTopic = expectedValuesByTopicName.get(topicName);
        assertNotNull("No expected values for " + topicName + " found", expectedValuesAndSchemasForTopic);
        assertRecordSchemaAndValues(expectedValuesAndSchemasForTopic, record, Envelope.FieldName.AFTER);
    }

    protected TestConsumer testConsumer2(int expectedRecordsCount, String... topicPrefixes) {
        return new TestConsumer(expectedRecordsCount, topicPrefixes);
   }

   protected class TestConsumer {
       private final ConcurrentLinkedQueue<SourceRecord> records;
       private int remainingCount;
       private final List<String> topicPrefixes;
       private boolean ignoreExtraRecords = false;

       protected TestConsumer(int expectedRecordsCount, String... topicPrefixes) {
           this.remainingCount = expectedRecordsCount;
           this.records = new ConcurrentLinkedQueue<>();
           this.topicPrefixes = Arrays.stream(topicPrefixes)
                   .map(p -> TestHelper.TEST_SERVER + "." + p)
                   .collect(Collectors.toList());
       }

       public void setIgnoreExtraRecords(boolean ignoreExtraRecords) {
           this.ignoreExtraRecords = ignoreExtraRecords;
       }

       private void poll() throws InterruptedException {
           while (remainingCount > 0) {
               List<DataChangeEvent> events = eventQueue.poll();
               for (DataChangeEvent event: events) {
                   final SourceRecord record = event.getRecord();
                   if ( ignoreTopic(record.topic()) ) {
                       return;
                   }
                   if (remainingCount == 0) {
                       if (ignoreExtraRecords) {
                           records.add(record);
                       } else {
                           fail("received more events than expected");
                       }
                   } else {
                       records.add(record);
                       remainingCount--;
                   }
               }
           }
           if (eventQueue.remainingCapacity() != eventQueue.totalCapacity()) {
               //
           }
       }

       private boolean ignoreTopic(String topicName) {
           if (topicPrefixes.isEmpty()) {
               return false;
           }

           for (String prefix : topicPrefixes) {
               if ( topicName.startsWith(prefix)) {
                   return false;
               }
           }

           return true;
       }

       protected void expects(int expectedRecordsCount) {
           assert remainingCount == 0;
           remainingCount = expectedRecordsCount;
       }

       protected SourceRecord remove() {
           return records.remove();
       }

       protected boolean isEmpty() {
           return records.isEmpty();
       }

       protected void process(Consumer<SourceRecord> consumer) {
           records.forEach(consumer);
       }

       protected void clear() {
           records.clear();
       }

       protected void await(long timeout, TimeUnit unit) throws InterruptedException {
           
//           if (!latch.await(timeout, unit)) {
//               fail("Consumer is still expecting " + latch.getCount() + " records, as it received only " + records.size());
//           }
           poll();
       }
   }
}
