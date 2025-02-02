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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.netflix.conductor.core.listener.WorkflowStatusListener;

import com.fasterxml.jackson.databind.ObjectMapper;

@Configuration
@EnableConfigurationProperties(KafkaWorkflowListenerProperties.class)
@ConditionalOnProperty(name = "conductor.workflow-status-listener.type", havingValue = "kafka")
public class KafkaWorkflowListenerConfiguration {

    private static final Logger LOGGER =
            LoggerFactory.getLogger(KafkaWorkflowListenerConfiguration.class);

    @Bean
    @Autowired
    public WorkflowStatusListener getWorkflowStatusListener(
            KafkaWorkflowProducerManager KafkaWorkflowProducerManager,
            KafkaWorkflowListenerProperties properties,
            ObjectMapper objectMapper) {
        LOGGER.info("Kafka workflow event listener is ENABLED.");
        return new KafkaWorkflowStatusListener(
                KafkaWorkflowProducerManager, properties, objectMapper);
    }
}
