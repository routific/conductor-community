/*
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

import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;

@SuppressWarnings("rawtypes")
@Component
public class KafkaWorkflowProducerManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaWorkflowProducerManager.class);

    private final String requestTimeoutConfig;
    private final Cache<Properties, Producer> kafkaProducerCache;
    private final String maxBlockMsConfig;
    private String topicNamespace = "";
    private String bootstrapServers;
    private String securityProtocol;
    private String saslMechanism;
    private String saslUsername;
    private String saslPassword;
    private String jaasTemplate;
    private String sslTruststorePath;
    private String sslTruststorePassword;

    private static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
    private static final RemovalListener<Properties, Producer> LISTENER = notification -> {
        if (notification.getValue() != null) {
            notification.getValue().close();
            LOGGER.info("Closed kafka workflow event producer.");
        }
    };

    @Autowired
    public KafkaWorkflowProducerManager(
            @Value("${conductor.tasks.kafka-publish.requestTimeout:100ms}") Duration requestTimeout,
            @Value("${conductor.tasks.kafka-publish.maxBlock:500ms}") Duration maxBlock,
            @Value("${conductor.tasks.kafka-publish.cacheSize:5}") int cacheSize,
            @Value("${conductor.tasks.kafka-publish.cacheTime:120000ms}") Duration cacheTime,
            @Value("${conductor.tasks.kafka-publish.bootstrapServers:localhost:9092}") String bootstrapServers,
            @Value("${conductor.tasks.kafka-publish.securityProtocol:SASL_SSL}") String securityProtocol,
            @Value("${conductor.tasks.kafka-publish.saslMechanism:PLAIN}") String saslMechanism,
            @Value("${conductor.tasks.kafka-publish.saslUsername:#{null}}") String saslUsername,
            @Value("${conductor.tasks.kafka-publish.saslPassword:#{null}}") String saslPassword,
            @Value("${conductor.tasks.kafka-publish.jaasTemplate:#{null}}") String jaasTemplate,
            @Value("${conductor.tasks.kafka-publish.topicNamespace:#{null}}") String topicNamespace,
            @Value("${conductor.tasks.kafka-publish.truststorePath:#{null}}") String truststorePath,
            @Value("${conductor.tasks.kafka-publish.truststorePassword:#{null}}") String truststorePassword) {
        this.requestTimeoutConfig = String.valueOf(requestTimeout.toMillis());
        this.maxBlockMsConfig = String.valueOf(maxBlock.toMillis());
        this.securityProtocol = securityProtocol;
        this.topicNamespace = topicNamespace;
        this.bootstrapServers = bootstrapServers;
        this.saslMechanism = saslMechanism;
        this.saslUsername = saslUsername;
        this.saslPassword = saslPassword;
        this.jaasTemplate = jaasTemplate;
        this.sslTruststorePath = truststorePath;
        this.sslTruststorePassword = truststorePassword;
        this.kafkaProducerCache = CacheBuilder.newBuilder()
                .removalListener(LISTENER)
                .maximumSize(cacheSize)
                .expireAfterAccess(cacheTime.toMillis(), TimeUnit.MILLISECONDS)
                .build();
    }

    public Producer getProducer() {
        Properties configProperties = getProducerProperties();
        return getFromCache(configProperties, () -> new KafkaProducer(configProperties));
    }

    public String getTopicNamespace() {
        return this.topicNamespace;
    }

    public String getBootstrapServers() {
        return this.bootstrapServers;
    }

    @VisibleForTesting
    Producer getFromCache(Properties configProperties, Callable<Producer> createProducerCallable) {
        try {
            return kafkaProducerCache.get(configProperties, createProducerCallable);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    Properties getProducerProperties() {

        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers);

        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,  StringSerializer.class.getCanonicalName());

        String requestTimeoutMs = requestTimeoutConfig;
        String maxBlockMs = maxBlockMsConfig;

        configProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs);
        configProperties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs);
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);

        if (Objects.nonNull(securityProtocol)) {
            configProperties.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
            configProperties.put(SaslConfigs.SASL_MECHANISM, saslMechanism);
            configProperties.put(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG, "");
            configProperties.put(
                    SaslConfigs.SASL_JAAS_CONFIG,
                    jaasTemplate
                            + " required username=\""
                            + saslUsername
                            + "\" password=\""
                            + saslPassword
                            + "\";");
        }

        if (!sslTruststorePath.isEmpty()) {
            configProperties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, sslTruststorePath);
            configProperties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, sslTruststorePassword);
        }

        return configProperties;
    }
}
