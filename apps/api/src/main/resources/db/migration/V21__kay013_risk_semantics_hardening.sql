ALTER TABLE risk_reviews
    DROP CONSTRAINT risk_reviews_status_check;

ALTER TABLE risk_reviews
    ADD CONSTRAINT risk_reviews_status_check CHECK (status IN ('OPEN', 'IN_REVIEW', 'RESOLVED', 'DISMISSED', 'BLOCKED'));

UPDATE risk_reviews rr
SET status = 'BLOCKED',
    closed_at = NULL
FROM risk_decisions rd
WHERE rr.workspace_id = rd.workspace_id
  AND rr.id = rd.review_id
  AND rd.outcome = 'BLOCK'
  AND rr.status = 'RESOLVED';
