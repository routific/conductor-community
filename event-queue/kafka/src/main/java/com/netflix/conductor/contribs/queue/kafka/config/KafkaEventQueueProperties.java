/*
 * Copyright 2022 Netflix, Inc.
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

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties("conductor.event-queues.kafka")
public class KafkaEventQueueProperties {
    private String truststorePath;
    private String truststorePassword;

    private String bootstrapServers = "localhost:9092";
    private String securityProtocol = "SASL_SSL";
    private String saslMechanism = "PLAIN";
    private String saslUsername;
    private String saslPassword;
    private String jaasTemplate = "org.apache.kafka.common.security.plain.PlainLoginModule";

    private String topicNamespace = "";
    private String consumerGroupId;

    private int pollIntervalInMs;
    private int pollTimeoutInMs;
    private boolean autoOffsetReset = false;
    private boolean autoCommit = false;

    private int kafkaThreadPoolCount = 20;

    public int getKafkaThreadPoolCount() {
        return kafkaThreadPoolCount;
    }

    public void setKafkaThreadPoolCount(int count) {
        this.kafkaThreadPoolCount = count;
    }

    public String getTruststorePath() {
        return truststorePath;
    }

    public void setTruststorePath(String truststorePath) {
        this.truststorePath = truststorePath;
    }

    public String getTruststorePassword() {
        return truststorePassword;
    }

    public void setTruststorePassword(String truststorePassword) {
        this.truststorePassword = truststorePassword;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getSecurityProtocol() {
        return securityProtocol;
    }

    public void setSecurityProtocol(String securityProtocol) {
        this.securityProtocol = securityProtocol;
    }

    public String getSaslMechanism() {
        return saslMechanism;
    }

    public void setSaslMechanism(String saslMechanism) {
        this.saslMechanism = saslMechanism;
    }

    public String getSaslUsername() {
        return saslUsername;
    }

    public void setSaslUsername(String username) {
        this.saslUsername = username;
    }

    public String getSaslPassword() {
        return saslPassword;
    }

    public void setSaslPassword(String password) {
        this.saslPassword = password;
    }

    public String getJaasTemplate() {
        return jaasTemplate;
    }

    public void setJaasTemplate(String jaasTemplate) {
        this.jaasTemplate = jaasTemplate;
    }

    public String getTopicNamespace() {
        return topicNamespace;
    }

    public void setTopicNamespace(String topicNamespace) {
        this.topicNamespace = topicNamespace;
    }

    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public void setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
    }

    public int getPollIntervalInMs() {
        return pollIntervalInMs;
    }

    public void setPollIntervalInMs(int pollIntervalInMs) {
        this.pollIntervalInMs = pollIntervalInMs;
    }

    public int getPollTimeoutInMs() {
        return pollTimeoutInMs;
    }

    public void setPollTimeoutInMs(int pollTimeoutInMs) {
        this.pollTimeoutInMs = pollTimeoutInMs;
    }

    public boolean getAutoOffsetReset() {
        return autoOffsetReset;
    }

    public void setAutoOffsetReset(boolean autoOffsetReset) {
        this.autoOffsetReset = autoOffsetReset;
    }

    public boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }
}
