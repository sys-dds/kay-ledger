CREATE OR REPLACE FUNCTION kay_ledger_prevent_journal_entry_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'journal_entries are immutable after creation';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION kay_ledger_prevent_journal_posting_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'journal_postings are immutable after creation';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER journal_entries_prevent_update
    BEFORE UPDATE ON journal_entries
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_prevent_journal_entry_mutation();

CREATE TRIGGER journal_entries_prevent_delete
    BEFORE DELETE ON journal_entries
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_prevent_journal_entry_mutation();

CREATE TRIGGER journal_postings_prevent_update
    BEFORE UPDATE ON journal_postings
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_prevent_journal_posting_mutation();

CREATE TRIGGER journal_postings_prevent_delete
    BEFORE DELETE ON journal_postings
    FOR EACH ROW
    EXECUTE FUNCTION kay_ledger_prevent_journal_posting_mutation();
