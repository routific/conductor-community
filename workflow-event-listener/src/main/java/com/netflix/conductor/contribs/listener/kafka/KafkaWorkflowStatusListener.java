/*
 * Copyright 2023 Netflix, Inc.
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
package com.netflix.conductor.contribs.listener.kafka;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.common.run.WorkflowSummary;
import com.netflix.conductor.core.listener.WorkflowStatusListener;
import com.netflix.conductor.model.WorkflowModel;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Provides default implementation of workflow Kafka immediately after workflow is completed or
 * terminated.
 *
 * @author pavel.halabala
 */
public class KafkaWorkflowStatusListener implements WorkflowStatusListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaWorkflowStatusListener.class);
    private final KafkaWorkflowProducerManager kafkaWorkflowProducerManager;
    private final KafkaWorkflowListenerProperties properties;
    private final ObjectMapper objectMapper;

    public KafkaWorkflowStatusListener(
            KafkaWorkflowProducerManager kafkaWorkflowProducerManager,
            KafkaWorkflowListenerProperties properties,
            ObjectMapper objectMapper) {
        this.kafkaWorkflowProducerManager = kafkaWorkflowProducerManager;
        this.properties = properties;
        this.objectMapper = objectMapper;
    }

    @Override
    public void onWorkflowStarted(WorkflowModel workflow) {
        try {
            String topicName =
                    kafkaWorkflowProducerManager.getTopicNamespace()
                            + this.properties.getStartedWorkflowTopic();
            LOGGER.info(
                    "Publishing kafka event for started workflow {} to topic {}",
                    workflow.getWorkflowId(),
                    topicName);
            this.publish(workflow, topicName);
        } catch (Exception e) {
            LOGGER.error("Failed to send workflow started event to kafka: {}", e.getMessage());
        }
    }

    @Override
    public void onWorkflowCompleted(WorkflowModel workflow) {
        try {
            String topicName =
                    kafkaWorkflowProducerManager.getTopicNamespace()
                            + this.properties.getCompletedWorkflowTopic();
            LOGGER.info(
                    "Publishing kafka event for completed workflow {} to topic {}",
                    workflow.getWorkflowId(),
                    topicName);
            this.publish(workflow, topicName);
        } catch (Exception e) {
            LOGGER.error("Failed to send workflow completed event to kafka: {}", e.getMessage());
        }
    }

    @Override
    public void onWorkflowTerminated(WorkflowModel workflow) {
        try {
            String topicName =
                    kafkaWorkflowProducerManager.getTopicNamespace()
                            + this.properties.getFailedWorkflowTopic();
            LOGGER.info(
                    "Publishing kafka event for terminated workflow {} to topic {}",
                    workflow.getWorkflowId(),
                    topicName);
            this.publish(workflow, topicName);
        } catch (Exception e) {
            LOGGER.error("Failed to send workflow failed event to kafka: {}", e.getMessage());
        }
    }

    private String workflowToMessage(WorkflowModel workflowModel) {
        String jsonWfSummary;
        WorkflowSummary summary = new WorkflowSummary(workflowModel.toWorkflow());
        try {
            jsonWfSummary = objectMapper.writeValueAsString(summary);
        } catch (JsonProcessingException e) {
            LOGGER.error(
                    "Failed to convert WorkflowSummary: {} to String. Exception: {}", summary, e);
            throw new RuntimeException(e);
        }
        return jsonWfSummary;
    }

    private Future<RecordMetadata> publish(WorkflowModel workflow, String topic) throws Exception {
        long startPublishingEpochMillis = Instant.now().toEpochMilli();
        Producer producer = kafkaWorkflowProducerManager.getProducer();
        long timeTakenToCreateProducer = Instant.now().toEpochMilli() - startPublishingEpochMillis;
        LOGGER.debug("Time taken getting producer {}", timeTakenToCreateProducer);

        Object key = workflow.getWorkflowId();

        Map<String, Object> headers = new HashMap<>();
        if (workflow.getCorrelationId() != null) {
            headers.put("traceparent", workflow.getCorrelationId());
        }
        if (workflow.getUserIdentifier() != null) {
            headers.put("X-ROUTIFIC-USER", workflow.getUserIdentifier());
        }

        String value = this.workflowToMessage(workflow);
        Iterable<Header> recordHeaders =
                headers.entrySet().stream()
                        .map(
                                header ->
                                        new RecordHeader(
                                                header.getKey(),
                                                String.valueOf(header.getValue()).getBytes()))
                        .collect(Collectors.toList());

        ProducerRecord rec = new ProducerRecord(topic, null, null, key, value, recordHeaders);

        Future send = producer.send(rec);

        long timeTakenToPublish = Instant.now().toEpochMilli() - startPublishingEpochMillis;
        LOGGER.debug("Time taken publishing {}", timeTakenToPublish);

        return send;
    }

    public static class Input {

        public static final String STRING_SERIALIZER = StringSerializer.class.getCanonicalName();
        private Map<String, Object> headers = new HashMap<>();
        private Object key;
        private Object value;
        private String topic;

        public Map<String, Object> getHeaders() {
            return headers;
        }

        public void setHeaders(Map<String, Object> headers) {
            this.headers = headers;
        }

        public Object getValue() {
            return value;
        }

        public void setValue(Object value) {
            this.value = value;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        @Override
        public String toString() {
            return "Input{"
                    + "headers="
                    + headers
                    + '\''
                    + ", key="
                    + key
                    + ", value="
                    + value
                    + ", topic='"
                    + topic
                    + '\''
                    + '}';
        }
    }
}
