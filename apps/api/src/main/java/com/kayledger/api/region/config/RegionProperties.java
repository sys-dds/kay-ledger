package com.kayledger.api.region.config;

import java.util.ArrayList;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kay-ledger.region")
public class RegionProperties {

    private String localRegionId = "region-a";
    private List<String> peerRegionIds = new ArrayList<>(List.of("region-b"));
    private String activeRegionLabel = "region-a";
    private String homeRegionMode = "OWNED_WORKSPACES";
    private boolean replicationProducerEnabled = true;
    private boolean replicationConsumerEnabled = true;
    private String replicationTopic = "kay-ledger.region.replication";

    public String getLocalRegionId() {
        return localRegionId;
    }

    public void setLocalRegionId(String localRegionId) {
        this.localRegionId = localRegionId;
    }

    public List<String> getPeerRegionIds() {
        return peerRegionIds;
    }

    public void setPeerRegionIds(List<String> peerRegionIds) {
        this.peerRegionIds = peerRegionIds == null ? new ArrayList<>() : new ArrayList<>(peerRegionIds);
    }

    public String getActiveRegionLabel() {
        return activeRegionLabel;
    }

    public void setActiveRegionLabel(String activeRegionLabel) {
        this.activeRegionLabel = activeRegionLabel;
    }

    public String getHomeRegionMode() {
        return homeRegionMode;
    }

    public void setHomeRegionMode(String homeRegionMode) {
        this.homeRegionMode = homeRegionMode;
    }

    public boolean isReplicationProducerEnabled() {
        return replicationProducerEnabled;
    }

    public void setReplicationProducerEnabled(boolean replicationProducerEnabled) {
        this.replicationProducerEnabled = replicationProducerEnabled;
    }

    public boolean isReplicationConsumerEnabled() {
        return replicationConsumerEnabled;
    }

    public void setReplicationConsumerEnabled(boolean replicationConsumerEnabled) {
        this.replicationConsumerEnabled = replicationConsumerEnabled;
    }

    public String getReplicationTopic() {
        return replicationTopic;
    }

    public void setReplicationTopic(String replicationTopic) {
        this.replicationTopic = replicationTopic;
    }
}
