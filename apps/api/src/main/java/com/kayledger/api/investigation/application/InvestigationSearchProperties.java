package com.kayledger.api.investigation.application;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@ConfigurationProperties(prefix = "kay-ledger.search.opensearch")
public class InvestigationSearchProperties {

    private String endpoint = "http://localhost:9200";
    private String investigationIndex = "kay-ledger-investigation";

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    public String getInvestigationIndex() {
        return investigationIndex;
    }

    public void setInvestigationIndex(String investigationIndex) {
        this.investigationIndex = investigationIndex;
    }
}
