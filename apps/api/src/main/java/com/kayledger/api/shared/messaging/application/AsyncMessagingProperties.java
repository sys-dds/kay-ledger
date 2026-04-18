package com.kayledger.api.shared.messaging.application;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "kay-ledger.async")
public class AsyncMessagingProperties {

    private final Kafka kafka = new Kafka();
    private final Relay relay = new Relay();

    public Kafka getKafka() {
        return kafka;
    }

    public Relay getRelay() {
        return relay;
    }

    public static class Kafka {
        private String topic = "kay-ledger.events";
        private String consumerGroupId = "kay-ledger-api-projections";

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public String getConsumerGroupId() {
            return consumerGroupId;
        }

        public void setConsumerGroupId(String consumerGroupId) {
            this.consumerGroupId = consumerGroupId;
        }
    }

    public static class Relay {
        private int batchSize = 25;
        private int maxAttempts = 3;
        private long backoffSeconds = 30;

        public int getBatchSize() {
            return batchSize;
        }

        public void setBatchSize(int batchSize) {
            this.batchSize = batchSize;
        }

        public int getMaxAttempts() {
            return maxAttempts;
        }

        public void setMaxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
        }

        public long getBackoffSeconds() {
            return backoffSeconds;
        }

        public void setBackoffSeconds(long backoffSeconds) {
            this.backoffSeconds = backoffSeconds;
        }
    }
}
