/*
 * Copyright 2017 Netflix, Inc.
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
package com.netflix.conductor.contribs.queue.kafka.config;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.netflix.conductor.contribs.queue.kafka.KafkaObservableQueue;
import com.netflix.conductor.core.events.EventQueueProvider;
import com.netflix.conductor.core.events.queue.ObservableQueue;

import rx.Scheduler;

/**
 * @author preeth, rickfish
 */
public class KafkaEventQueueProvider implements EventQueueProvider {
    private static Logger logger = LoggerFactory.getLogger(KafkaEventQueueProvider.class);
    protected Map<String, KafkaObservableQueue> queues = new ConcurrentHashMap<>();
    private KafkaEventQueueProperties properties;

    private final Scheduler scheduler;

    public KafkaEventQueueProvider(KafkaEventQueueProperties properties, Scheduler scheduler) {
        this.scheduler = scheduler;
        this.properties = properties;
        logger.info("Kafka Event Queue Provider initialized.");
    }

    @Override
    public String getQueueType() {
        return "kafka";
    }

    @Override
    public ObservableQueue getQueue(String queueURI) {
        return queues.computeIfAbsent(
                queueURI, q -> new KafkaObservableQueue(queueURI, properties, scheduler));
    }
}
