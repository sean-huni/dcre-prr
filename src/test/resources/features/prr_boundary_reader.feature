# PRR is the single writer of the PAYMENTS spine (R-04): it reads one OnHost
# ENDO payment request file per arrival and persists the header plus every
# detail line into dcre_pay, keyed (arrival_id, sequence) for restart safety
# (R-05). There is no flow scenario here and there cannot be one: the database
# is the family discriminator now, so tx_header has no flow column to assert on.
@prr
Feature: PRR boundary reader ingests OnHost ENDO payment request files
  The boundary reader parses a fixed-width payment request file, applies
  the structural file-fatal tier (R-19) and persists the transaction spine.

  Scenario: A valid V2 payment file is persisted into the transaction spine
    Given the boundary reader receives the standard V2 payment file
    When the PRR job runs
    Then the job completes with a clean business verdict
    And the spine holds one header row and 30 entry rows for the arrival
    And every persisted amount equals its raw digits scaled to two decimals

  Scenario: Re-processing the same arrival leaves the spine unchanged
    Given the boundary reader receives the standard V2 payment file
    When the PRR job runs
    And the PRR job runs again for the same arrival
    Then the job completes with a clean business verdict
    And the spine holds one header row and 30 entry rows for the arrival

  # The split's structural invariant, asserted against the SCHEMA rather than a
  # value: CRR would answer this query with 'COL' or 'PAY'. PRR's spine cannot
  # answer it at all, which is the whole point.
  Scenario: The payments spine has no flow column to discriminate on
    Given the boundary reader receives the standard V2 payment file
    When the PRR job runs
    Then the job completes with a clean business verdict
    And the tx_header table has no flow column

  Scenario: A header declaring the wrong transaction count is rejected file-fatally
    Given the boundary reader receives the V2 file with a header declaring 31 transactions
    When the PRR job runs
    Then the file is rejected file-fatally with a reason containing "tx_count=31"
    And no spine entries are persisted for the arrival

  Scenario: A malformed short header is rejected file-fatally
    Given the boundary reader receives the V2 file with a truncated header
    When the PRR job runs
    Then the file is rejected file-fatally with a reason containing "header shorter"
    And no spine entries are persisted for the arrival

  # The reason below LEAVES THE BUILDING: HeaderService -> HeaderTasklet ->
  # executionContext["fileFatalReason"] -> the initial-response service, written
  # verbatim into the client's NACK file. SCRUM-107 moved this verdict from
  # LineRangePartitioner to the header stage, which changed the wording a client
  # sees for a ragged book, so it is pinned here.
  Scenario: A ragged final detail record is rejected file-fatally naming its LRECL
    Given the boundary reader receives the V2 file with a truncated final detail
    When the PRR job runs
    Then the file is rejected file-fatally with a reason containing "detail LRECL 100 matches no layout"
    And no spine entries are persisted for the arrival

  Scenario: A filename contradicting the header destination is rejected file-fatally
    Given the boundary reader receives the standard V2 payment file
    And the file arrived under the name "FNBXX99_DCRERF2026071112000002.txt"
    When the PRR job runs
    Then the file is rejected file-fatally with a reason containing "R-31 mismatch"
    And no spine entries are persisted for the arrival

  Scenario: The legacy V1 layout fails closed while disabled by default
    Given the boundary reader receives the legacy V1 payment file
    When the PRR job runs
    Then the file is rejected file-fatally with a reason containing "V1 layout fails closed"
    And no spine entries are persisted for the arrival
