/*
 * Copyright 2016 Netflix, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.contribs.queue.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RoundRobinAssignor;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.contribs.queue.kafka.config.KafkaEventQueueProperties;
import com.netflix.conductor.core.events.queue.Message;
import com.netflix.conductor.core.events.queue.ObservableQueue;
import com.netflix.conductor.core.events.queue.ObservableQueueHandler;
import com.netflix.conductor.core.utils.Utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import rx.Observable;
import rx.Scheduler;

public class KafkaObservableQueue implements ObservableQueue, Runnable, ConsumerRebalanceListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaObservableQueue.class);

    private static final String QUEUE_TYPE = "kafka";

    private final String queueName;

    private final int pollIntervalInMs;

    private final int pollTimeoutInMs;

    private final Boolean autoCommit;

    private final String saslMechanismConfig;

    private final String securityProtocol;

    private final String truststorePath;
    private final String truststorePassword;

    private KafkaConsumer<String, String> consumer;

    private final String saslUsernameConfig;
    private final String saslPasswordConfig;

    private final String kafkaNamespace;
    private final String jaasTemplate;

    private final AtomicBoolean running = new AtomicBoolean(false);

    private final ExecutorService executor;

    private Map<TopicPartition, KafkaPartitionTask> activeTasks = new HashMap<>();
    private final Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
    private long lastCommitTime = System.currentTimeMillis();

    private ObservableQueueHandler handler;

    public KafkaObservableQueue(
            String queueName, KafkaEventQueueProperties properties, Scheduler scheduler) {
        this.kafkaNamespace = properties.getTopicNamespace();
        this.jaasTemplate = properties.getJaasTemplate();
        this.queueName = queueName;
        this.pollIntervalInMs = properties.getPollIntervalInMs();
        this.pollTimeoutInMs = properties.getPollTimeoutInMs();
        this.securityProtocol = properties.getSecurityProtocol();
        this.truststorePath = properties.getTruststorePath();
        this.truststorePassword = properties.getTruststorePassword();
        // this.autoOffset = properties.getAutoOffsetReset();
        this.autoCommit = properties.getAutoCommit();
        this.saslMechanismConfig = properties.getSaslMechanism();

        this.saslUsernameConfig = properties.getSaslUsername();
        this.saslPasswordConfig = properties.getSaslPassword();

        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("kafka-record-process-thread-%d").build();
        this.executor =
                Executors.newFixedThreadPool(properties.getKafkaThreadPoolCount(), threadFactory);

        init(properties);
    }

    /**
     * Initializes the kafka producer with the defaults. Fails in case of any mandatory configs are
     * missing.
     *
     * @param config
     */
    private void init(KafkaEventQueueProperties properties) {
        try {
            Properties consumerProperties = new Properties();

            String prop = properties.getBootstrapServers();
            if (prop != null) {
                consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, prop);
            }
            prop = properties.getConsumerGroupId();
            if (prop != null) {
                consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, prop);
            }

            // consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffset);
            consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
            consumerProperties.put(
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            consumerProperties.put(
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            consumerProperties.put(
                    ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG,
                    RoundRobinAssignor.class.getName());

            if (Objects.nonNull(securityProtocol)) {
                consumerProperties.put(
                        SaslConfigs.SASL_JAAS_CONFIG,
                        jaasTemplate
                                + " required username=\""
                                + saslUsernameConfig
                                + "\" password=\""
                                + saslPasswordConfig
                                + "\";");

                consumerProperties.put(
                        CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
                consumerProperties.put(SaslConfigs.SASL_MECHANISM, saslMechanismConfig);
                consumerProperties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            }

            if (!truststorePath.isEmpty()) {
                consumerProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, truststorePath);
                consumerProperties.put(
                        SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, truststorePassword);
            }

            checkConsumerProps(consumerProperties);

            consumerProperties.put(
                    ConsumerConfig.CLIENT_ID_CONFIG,
                    queueName + "_consumer_" + Utils.getServerId() + "_0");

            this.consumer = new KafkaConsumer<String, String>(consumerProperties);
            consumer.subscribe(Collections.singletonList(kafkaNamespace + queueName), this);

        } catch (KafkaException e) {
            e.printStackTrace();
        }
    }

    /**
     * Checks mandatory configs are available for kafka consumer.
     *
     * @param consumerProps
     */
    private void checkConsumerProps(Properties consumerProps) {
        List<String> mandatoryKeys =
                Arrays.asList(
                        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
        List<String> keysNotFound = hasKeyAndValue(consumerProps, mandatoryKeys);
        if (keysNotFound != null && keysNotFound.size() > 0) {
            logger.error("Configuration missing for Kafka consumer. {}" + keysNotFound.toString());
            throw new RuntimeException(
                    "Configuration missing for Kafka consumer." + keysNotFound.toString());
        }
    }

    /**
     * Validates whether the property has given keys.
     *
     * @param prop
     * @param keys
     * @return
     */
    private List<String> hasKeyAndValue(Properties prop, List<String> keys) {
        List<String> keysNotFound = new ArrayList<>();
        for (String key : keys) {
            if (!prop.containsKey(key) || Objects.isNull(prop.get(key))) {
                keysNotFound.add(key);
            }
        }
        return keysNotFound;
    }

    private void checkActiveTasks() {
        List<TopicPartition> finishedTasksPartitions = new ArrayList<>();
        activeTasks.forEach(
                (partition, task) -> {
                    if (task.isFinished()) finishedTasksPartitions.add(partition);
                    long offset = task.getCurrentOffset();
                    if (offset > 0) offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
                });
        finishedTasksPartitions.forEach(
                partition -> {
                    logger.info("Resuming consume partitions: {}", partition.toString());
                    activeTasks.remove(partition);
                });
        consumer.resume(finishedTasksPartitions);
    }

    private void commitOffsets() {
        try {
            long currentTimeMillis = System.currentTimeMillis();
            if (currentTimeMillis - lastCommitTime > 5000) {
                if (!offsetsToCommit.isEmpty()) {
                    consumer.commitSync(offsetsToCommit);
                    offsetsToCommit.clear();
                }
                lastCommitTime = currentTimeMillis;
            }
        } catch (Exception e) {
            logger.error("Failed to commit offsets!", e);
        }
    }

    private void handleFetchedRecords(
            ConsumerRecords<String, String> records, ObservableQueueHandler handler) {
        if (records.count() > 0) {
            logger.info(
                    "Polled {} messages from kafka topic: {}.",
                    records.count(),
                    kafkaNamespace + queueName);

            List<TopicPartition> partitionsToPause = new ArrayList<>();
            records.partitions()
                    .forEach(
                            partition -> {
                                List<ConsumerRecord<String, String>> partitionRecords =
                                        records.records(partition);
                                KafkaPartitionTask task =
                                        new KafkaPartitionTask(partitionRecords, handler);
                                partitionsToPause.add(partition);
                                executor.submit(task);
                                activeTasks.put(partition, task);
                                logger.info("Pausing consume partitions: {}", partition.toString());
                            });

            consumer.pause(partitionsToPause);
        }
    }

    @Override
    public Observable<Message> observe() {
        return Observable.empty();
    }

    @Override
    public void observe(ObservableQueueHandler handler) {
        this.handler = handler;
        logger.info("Start observing kafka: {}", queueName);
        new Thread(this).start();
    }

    @Override
    public void run() {
        logger.info("Start kafka consumer: {}", queueName);
        try {
            while (isRunning()) {
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(pollTimeoutInMs));
                handleFetchedRecords(records, handler);
                checkActiveTasks();
                commitOffsets();
            }
        } catch (WakeupException we) {
            if (isRunning()) throw we;
        } finally {
            consumer.close();
        }
        logger.info("Ended kafka consumer: {}", queueName);
    }

    @Override
    public List<String> ack(List<Message> messages) {
        List<String> messageIds = new ArrayList<String>();

        return messageIds;
    }

    public void setUnackTimeout(Message message, long unackTimeout) {}

    @Override
    public long size() {
        return 0;
    }

    @Override
    public String getType() {
        return QUEUE_TYPE;
    }

    @Override
    public String getName() {
        return queueName;
    }

    @Override
    public String getURI() {
        return queueName;
    }

    @Override
    public void start() {
        logger.info("Started listening to {}:{}", getClass().getSimpleName(), queueName);
        running.set(true);
    }

    @Override
    public void stop() {
        logger.info("Stopped listening to {}:{}", getClass().getSimpleName(), queueName);
        running.set(false);
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public void publish(List<Message> messages) {
        // todo: implement DLQ
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {

        // 1. Stop all tasks handling records from revoked partitions
        Map<TopicPartition, KafkaPartitionTask> stoppedTask = new HashMap<>();
        for (TopicPartition partition : partitions) {
            KafkaPartitionTask task = activeTasks.remove(partition);
            if (task != null) {
                task.stop();
                stoppedTask.put(partition, task);
            }
        }

        // 2. Wait for stopped tasks to complete processing of current record
        stoppedTask.forEach(
                (partition, task) -> {
                    long offset = task.waitForCompletion();
                    if (offset > 0) offsetsToCommit.put(partition, new OffsetAndMetadata(offset));
                });

        // 3. collect offsets for revoked partitions
        Map<TopicPartition, OffsetAndMetadata> revokedPartitionOffsets = new HashMap<>();
        partitions.forEach(
                partition -> {
                    OffsetAndMetadata offset = offsetsToCommit.remove(partition);
                    if (offset != null) revokedPartitionOffsets.put(partition, offset);
                });

        // 4. commit offsets for revoked partitions
        try {
            consumer.commitSync(revokedPartitionOffsets);
        } catch (Exception e) {
            logger.warn("Failed to commit offsets for revoked partitions!");
        }
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        consumer.resume(partitions);
    }

    @Override
    public void close() {
        running.set(false);
        consumer.unsubscribe();
        consumer.close();
    }
}
