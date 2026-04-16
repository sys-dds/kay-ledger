package com.kayledger.api.finance.model;

import java.util.List;

public record JournalEntryDetails(JournalEntry journalEntry, List<JournalPosting> postings) {
}
