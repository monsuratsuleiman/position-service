# End-to-End Test Plan
## Position Service — Backend + UI

**Version:** 1.0
**Date:** February 7, 2026
**Covers:** position-service (Kotlin/Ktor backend) + position-service-ui (React/TypeScript frontend)

---

## 1. Overview

This plan validates the full system from trade event ingestion through Kafka processing to database persistence and UI rendering. Tests are organized by functional flow, mirroring the ARD architecture.

### System Under Test

```
Trade Event (Kafka)
  → Trade Consumer (store + publish 2 calc requests)
    → Kafka (position-calculation-requests)
      → Calculation Worker (incremental/full + WAC)
        → PostgreSQL (snapshots + prices + history)
          → REST API (/api/v1/positions/*)
            → UI (React dashboard)
```

### Prerequisites

| Dependency | Setup |
|---|---|
| PostgreSQL 16 | docker-compose up (port 5432) |
| Kafka + Zookeeper | docker-compose up (port 9092) |
| Topics | `trade-events` (10p), `position-calculation-requests` (20p) |
| Backend | `./gradlew run` (port 8080) |
| UI dev server | `npm run dev` (port 5173) |

---

## 2. Test Categories

| Category | Scope | Environment |
|---|---|---|
| **E2E-FLOW** | Full pipeline: Kafka → DB → API → UI | All services running |
| **E2E-CALC** | Calculation correctness through the stack | Backend + DB + Kafka |
| **E2E-API** | REST API contract with real data | Backend + DB |
| **E2E-UI** | UI renders correct data from live API | All services running |
| **E2E-PERF** | Latency and throughput under load | All services running |
| **E2E-RESIL** | Failure recovery and edge cases | All services + fault injection |

---

## 3. E2E-FLOW: Full Pipeline Tests

These validate the complete trade-to-screen path.

### E2E-FLOW-001: Single Trade — Happy Path

**Objective:** One trade event produces 2 position snapshots visible in UI.

**Steps:**
1. Publish a TradeEvent to `trade-events` topic:
   ```json
   {
     "sequenceNum": 1001,
     "book": "EQUITY-1",
     "counterparty": "GOLDMAN",
     "instrument": "AAPL",
     "signedQuantity": 1000,
     "price": 150.000000,
     "tradeTime": "2026-02-07T10:30:00Z",
     "tradeDate": "2026-02-07",
     "settlementDate": "2026-02-09",
     "source": "OMS",
     "sourceId": "TRD-001"
   }
   ```
2. Wait for processing (poll API until snapshot exists, max 5s).
3. Query `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL/2026-02-07?dateBasis=TRADE_DATE`
4. Query `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL/2026-02-09?dateBasis=SETTLEMENT_DATE`
5. Open UI position dashboard, search for `EQUITY-1#GOLDMAN#AAPL`.

**Expected Results:**
- Trade date snapshot: netQuantity=1000, grossLong=1000, grossShort=0, tradeCount=1, WAC=$150.00
- Settlement date snapshot: same metrics on 2026-02-09
- `position_keys` row: last_trade_date=2026-02-07, last_settlement_date=2026-02-09
- UI displays both snapshots correctly
- `calculation_method` = "FULL_RECALC" (no previous snapshot)

---

### E2E-FLOW-002: Multiple Trades — Same Position, Same Day

**Objective:** Accumulation within a single business date.

**Steps:**
1. Publish 3 trades (same book/cpty/instrument, same trade_date):
   - Buy 1000 @ $150 (seq 2001)
   - Buy 500 @ $160 (seq 2002)
   - Sell 400 @ $155 (seq 2003)
2. Wait for all 3 to process.
3. Query trade date position.
4. Verify in UI.

**Expected Results:**
- netQuantity = 1100 (1000 + 500 - 400)
- grossLong = 1500
- grossShort = 400
- tradeCount = 3
- WAC = $153.333333 (weighted avg of buys, sell doesn't change WAC)
- `calculation_version` >= 3 (recalculated per trade)
- UI chart shows correct totals

---

### E2E-FLOW-003: Multi-Day Position Building

**Objective:** Incremental calculation across business dates.

**Steps:**
1. Day 1 (2026-02-02): Buy 1000 @ $150 (seq 3001)
2. Day 2 (2026-02-03): Buy 500 @ $160 (seq 3002)
3. Day 3 (2026-02-04): Sell 300 @ $155 (seq 3003)
4. Query each date.
5. Verify time series in UI.

**Expected Results:**
| Date | Net Qty | Gross Long | Gross Short | WAC | Method |
|---|---|---|---|---|---|
| 2026-02-02 | 1000 | 1000 | 0 | $150.00 | FULL_RECALC |
| 2026-02-03 | 1500 | 1500 | 0 | $153.33 | INCREMENTAL |
| 2026-02-04 | 1200 | 1500 | 300 | $153.33 | INCREMENTAL |

- Day 2 and Day 3 use INCREMENTAL method (previous snapshot exists)
- UI time series chart renders all 3 dates correctly

---

### E2E-FLOW-004: Dual Publishing — Trade + Settlement

**Objective:** Every trade creates snapshots on both date bases.

**Steps:**
1. Publish trade: tradeDate=2026-02-05, settlementDate=2026-02-07, qty=500, price=$200
2. Wait for processing.
3. Query both date bases.

**Expected Results:**
- `position_snapshots` has row for business_date=2026-02-05
- `position_snapshots_settled` has row for business_date=2026-02-07
- Both have identical metrics (same single trade)
- UI date basis toggle shows correct data for each view

---

### E2E-FLOW-005: Idempotent Trade Processing

**Objective:** Duplicate trade events are handled gracefully.

**Steps:**
1. Publish trade with sequenceNum=5001.
2. Wait for processing.
3. Publish the same trade again (identical sequenceNum=5001).
4. Wait 2s.
5. Query position.

**Expected Results:**
- Only 1 row in `position_trades` for sequence_num=5001
- Position snapshot unchanged (no double-counting)
- No errors in application logs

---

## 4. E2E-CALC: Calculation Correctness

### E2E-CALC-001: WAC Rule 1 — Moving Away from Zero (Buy into Long)

**Steps:**
1. From flat position, buy 1000 @ $150 (seq 6001).
2. Buy 500 @ $160 (seq 6002).

**Expected:** WAC = (150000 + 80000) / 1500 = $153.333333

---

### E2E-CALC-002: WAC Rule 2 — Moving Towards Zero (Sell from Long)

**Steps:**
1. Start with position from E2E-CALC-001 (qty=1500, WAC=$153.33).
2. Sell 400 @ $170 (seq 6003).

**Expected:**
- netQuantity = 1100
- WAC = $153.333333 (UNCHANGED — selling does not alter WAC)
- totalCostBasis = 153.333333 × 1100 = $168,666.666300

---

### E2E-CALC-003: WAC Rule 3 — Cross Zero (Long to Short)

**Steps:**
1. Start with position: qty=500, WAC=$150.
2. Sell 800 @ $160 (seq 7001).

**Expected:**
- netQuantity = -300
- WAC = $160.000000 (RESET to trade price)
- totalCostBasis = 160 × 300 = $48,000

---

### E2E-CALC-004: WAC Rule 4 — Position to Zero

**Steps:**
1. Start with position: qty=500, WAC=$150.
2. Sell 500 @ $155 (seq 8001).

**Expected:**
- netQuantity = 0
- WAC = $0 (flat position)
- totalCostBasis = $0

---

### E2E-CALC-005: WAC — Short Position Building

**Steps:**
1. From flat, sell 1000 @ $100 (seq 9001) — opening short.
2. Sell 500 @ $110 (seq 9002) — adding to short.

**Expected:**
- netQuantity = -1500
- totalCostBasis = -(100000 + 55000) = -$155,000
- WAC = |155000| / 1500 = $103.333333

---

### E2E-CALC-006: WAC — First Trade from Flat

**Objective:** Edge case where qty=0 and first trade must use trade price directly.

**Steps:**
1. Ensure no previous position exists for this key.
2. Buy 1000 @ $250 (seq 10001).

**Expected:**
- WAC = $250.000000 (trade price used directly, not averaged with zero)

---

### E2E-CALC-007: Late Trade — Cascade Recalculation

**Objective:** Late trade triggers recalculation of all affected dates.

**Steps:**
1. Insert trades building positions for Jan 20-25:
   - Jan 20: Buy 100 @ $50 (seq 11001)
   - Jan 22: Buy 200 @ $55 (seq 11002)
   - Jan 25: Buy 150 @ $52 (seq 11003)
2. Record current snapshots for Jan 20-25.
3. Submit late trade: tradeDate=Jan 21, Buy 300 @ $48 (seq 11004, tradeTime = now).
4. Wait for cascade processing.
5. Query all dates Jan 20-25.

**Expected:**
- Jan 20: UNCHANGED (before late trade date)
- Jan 21: NEW snapshot created (netQuantity includes new 300 buy)
- Jan 22-25: RECALCULATED with incremented `calculation_version`
- History table has entries with `change_reason='LATE_TRADE'` for Jan 22-25
- `superseded_at` populated on old versions
- Cascade does NOT extend beyond Jan 25 (last_trade_date optimization)

---

### E2E-CALC-008: Late Trade — Settlement Date Cascade

**Objective:** Late trade cascades on settlement date dimension too.

**Steps:**
1. Build positions with settlement dates spanning Jan 22 - Feb 5.
2. Submit late trade with settlementDate=Jan 23.
3. Verify settlement snapshots recalculated Jan 23 through Feb 5.

**Expected:**
- `position_snapshots_settled` recalculated for all dates in range
- `position_snapshots_settled_history` has LATE_TRADE entries
- Smart boundary: cascade stops at `last_settlement_date`, not today

---

### E2E-CALC-009: Incremental vs Full Recalculation

**Objective:** Verify the system correctly chooses incremental or full.

**Steps:**
1. Create position for Day 1 (no previous exists).
2. Create position for Day 2 (Day 1 exists).
3. Check `calculation_method` on both.

**Expected:**
- Day 1: `calculation_method = "FULL_RECALC"`
- Day 2: `calculation_method = "INCREMENTAL"`

---

### E2E-CALC-010: No Trades Day — Copy Forward

**Objective:** If no trades on a date, position carries forward from previous.

**Steps:**
1. Trade on Jan 20 (Day 1).
2. Trigger calc request for Jan 21 (no trades).
3. Query Jan 21 position.

**Expected:**
- Jan 21 snapshot = Jan 20 snapshot values (carried forward)
- WAC unchanged
- tradeCount = same as Jan 20 (cumulative, no new trades)

---

## 5. E2E-API: REST API Contract Tests

### E2E-API-001: GET /health

**Request:** `GET /api/v1/health`

**Expected:**
```json
{"status": "UP"}
```
- Status code: 200

---

### E2E-API-002: GET Position by Key + Date (Trade Date)

**Request:** `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL/2026-02-07?dateBasis=TRADE_DATE`

**Expected Response Structure:**
```json
{
  "positionKey": "EQUITY-1#GOLDMAN#AAPL",
  "businessDate": "2026-02-07",
  "netQuantity": 1000,
  "grossLong": 1000,
  "grossShort": 0,
  "tradeCount": 1,
  "totalNotional": 150000.000000,
  "calculationVersion": 1,
  "calculatedAt": "2026-02-07T...",
  "calculationMethod": "FULL_RECALC",
  "lastSequenceNum": 1001,
  "prices": [
    {
      "priceMethod": "WAC",
      "price": 150.000000,
      "methodData": {
        "totalCostBasis": 150000.000000,
        "lastUpdatedSequence": 1001
      }
    }
  ]
}
```
- Status code: 200

---

### E2E-API-003: GET Position by Key + Date (Settlement Date)

**Request:** `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL/2026-02-09?dateBasis=SETTLEMENT_DATE`

**Expected:** Same structure, pulls from `position_snapshots_settled`.

---

### E2E-API-004: GET Position — Not Found

**Request:** `GET /api/v1/positions/NONEXISTENT#KEY/2026-01-01`

**Expected:**
- Status code: 404
- Body: error message indicating position not found

---

### E2E-API-005: GET Position Time Series

**Request:** `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL?from=2026-02-01&to=2026-02-07&dateBasis=TRADE_DATE`

**Expected:**
- Array of PositionSummaryResponse objects
- Ordered by businessDate ascending
- One entry per date that has a snapshot
- Status code: 200

---

### E2E-API-006: GET Position History (Bitemporal)

**Request:** `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL/2026-02-07/history?dateBasis=TRADE_DATE`

**Expected:**
- Array of history entries ordered by `calculationVersion`
- Each entry includes: calculationVersion, calculatedAt, supersededAt, changeReason
- Current version has `supersededAt = null`
- Previous versions have `supersededAt` populated
- Status code: 200

---

### E2E-API-007: Invalid Date Format

**Request:** `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL/not-a-date`

**Expected:**
- Status code: 400
- Error message about invalid date format

---

### E2E-API-008: Invalid Date Basis Parameter

**Request:** `GET /api/v1/positions/EQUITY-1#GOLDMAN#AAPL/2026-02-07?dateBasis=INVALID`

**Expected:**
- Status code: 400
- Error message about invalid dateBasis enum value

---

## 6. E2E-UI: Frontend Integration Tests

> **Note:** The UI (position-service-ui) is in early scaffolding phase. These tests define the target behavior the UI must support. Update test selectors once components are built.

### E2E-UI-001: Position Dashboard — Load & Display

**Steps (Playwright):**
1. Navigate to `http://localhost:5173`.
2. Verify dashboard page loads without console errors.
3. Verify position table or grid is rendered.
4. Verify at least one position row is displayed (seed data required).

**Expected:**
- Page title or header contains "Position" text
- Table/grid shows columns: Position Key, Date, Net Qty, Gross Long, Gross Short, WAC
- No uncaught exceptions in console

---

### E2E-UI-002: Position Search / Filter

**Steps:**
1. Enter position key `EQUITY-1#GOLDMAN#AAPL` in search field.
2. Submit search.
3. Verify filtered results.

**Expected:**
- Only positions matching the key are displayed
- Results update without full page reload

---

### E2E-UI-003: Date Basis Toggle (Trade vs Settlement)

**Steps:**
1. View position for a key that has both trade and settlement snapshots.
2. Toggle date basis from "Trade Date" to "Settlement Date".
3. Verify data changes.

**Expected:**
- Trade date view shows snapshots from `position_snapshots`
- Settlement date view shows snapshots from `position_snapshots_settled`
- Business dates differ (trade date vs settlement date)
- Metrics may differ if different trades settle on different dates

---

### E2E-UI-004: Position Detail — WAC & Cost Basis

**Steps:**
1. Click into position detail for `EQUITY-1#GOLDMAN#AAPL` on a specific date.
2. Verify WAC price and cost basis are displayed.

**Expected:**
- WAC price shown with 6 decimal precision
- Total cost basis shown
- Price method labeled "WAC"

---

### E2E-UI-005: Time Series View

**Steps:**
1. Select position key.
2. Set date range (from/to).
3. Verify time series chart or table is rendered.

**Expected:**
- Multiple dates displayed in chronological order
- Net quantity line/bars show trend
- Date range filter applies correctly

---

### E2E-UI-006: Position History (Audit Trail)

**Steps:**
1. Navigate to position that has been recalculated (e.g., after late trade).
2. Open history/audit view.
3. Verify multiple calculation versions are shown.

**Expected:**
- Version history displayed with: version number, calculated_at, change_reason
- Current version highlighted or marked
- Previous versions show superseded_at timestamp
- Change reason (INITIAL, LATE_TRADE) displayed

---

### E2E-UI-007: Real-Time Update After Trade

**Steps:**
1. Open UI with position displayed.
2. Publish a new trade event to Kafka for that position.
3. Refresh or wait for auto-update.
4. Verify new quantities reflect.

**Expected:**
- Position metrics updated after trade processing
- New trade count incremented
- WAC recalculated if applicable

---

### E2E-UI-008: Empty State — No Positions

**Steps:**
1. Search for a position key that doesn't exist.

**Expected:**
- Friendly "No positions found" message
- No broken table/chart rendering
- No console errors

---

### E2E-UI-009: Error State — API Unavailable

**Steps:**
1. Stop the backend service.
2. Attempt to load positions in UI.

**Expected:**
- User-facing error message (not raw stack trace)
- Retry option or instruction
- Graceful degradation

---

## 7. E2E-PERF: Performance Tests

### E2E-PERF-001: Trade Ingestion Latency

**Objective:** Validate <5ms p95 trade ingestion.

**Steps:**
1. Publish 10,000 trade events with timestamps.
2. Measure time from Kafka publish to `position_trades` insert.

**Pass Criteria:** p95 < 5ms

---

### E2E-PERF-002: Incremental Calculation Latency

**Objective:** Validate <10ms p95 incremental calculation.

**Steps:**
1. Pre-seed positions for 100 position keys across 30 days.
2. Publish 1,000 new trades (1 per position per day).
3. Measure time from calc request consumed to snapshot saved.

**Pass Criteria:** p95 < 10ms, >90% use INCREMENTAL method

---

### E2E-PERF-003: Full Calculation Latency

**Objective:** Validate <50ms p95 full recalculation.

**Steps:**
1. Publish trades for a brand-new position key (no previous snapshots).
2. Measure full calculation time.

**Pass Criteria:** p95 < 50ms

---

### E2E-PERF-004: Position Query Latency

**Objective:** Validate <1ms p95 for current position lookup.

**Steps:**
1. Pre-seed 10,000 positions.
2. Run 50,000 random GET queries against `/api/v1/positions/{key}/{date}`.
3. Measure response time.

**Pass Criteria:** p95 < 1ms (excluding network; measured at server)

---

### E2E-PERF-005: Position With Prices Query

**Objective:** Validate <5ms p95 for position + prices.

**Steps:**
1. Same as PERF-004 but include price data in response.

**Pass Criteria:** p95 < 5ms

---

### E2E-PERF-006: Late Trade Cascade (6 Dates)

**Objective:** Validate <80ms total for 6-date cascade.

**Steps:**
1. Build 6-date position history.
2. Submit late trade that triggers 6-date cascade.
3. Measure total time from trade arrival to all 6 snapshots updated.

**Pass Criteria:** Total < 80ms (1 full + 5 incremental)

---

### E2E-PERF-007: Throughput — Sustained Load

**Objective:** Validate 10,000 trades/sec sustained.

**Steps:**
1. Publish trade events at 10,000/sec for 60 seconds (600,000 total).
2. Monitor Kafka consumer lag.
3. Verify all positions eventually calculated.

**Pass Criteria:**
- Consumer lag stays < 10,000
- All 600,000 trades processed within 90 seconds
- No OOM or connection pool exhaustion

---

### E2E-PERF-008: UI Page Load

**Objective:** Dashboard loads within acceptable time.

**Steps:**
1. Pre-seed 500 positions.
2. Measure time from navigation to first meaningful paint.

**Pass Criteria:** < 2 seconds for initial render, < 500ms for subsequent interactions

---

## 8. E2E-RESIL: Resilience & Edge Cases

### E2E-RESIL-001: Worker Crash Recovery

**Steps:**
1. Publish 100 trades while calculation worker is running.
2. Kill the worker mid-processing.
3. Restart the worker.
4. Verify all positions eventually calculated.

**Expected:**
- Kafka rebalances partitions
- Uncommitted offsets are re-consumed
- No missing or corrupt position snapshots
- No duplicate snapshots (idempotent save)

---

### E2E-RESIL-002: Database Connection Loss

**Steps:**
1. Start processing trades.
2. Temporarily block DB connections (iptables or pause Docker container).
3. Restore after 10 seconds.
4. Verify recovery.

**Expected:**
- HikariCP detects connection loss
- Application logs connection errors (no crash)
- Processing resumes after DB comes back
- No data loss

---

### E2E-RESIL-003: Kafka Broker Restart

**Steps:**
1. Start processing trades.
2. Restart one Kafka broker.
3. Verify processing continues.

**Expected:**
- Consumer reconnects automatically
- No message loss (replication factor handles it)
- Consumer lag spike then recovers

---

### E2E-RESIL-004: Out-of-Order Trade Events

**Steps:**
1. Publish trades with non-sequential sequence numbers:
   - seq 15003 first
   - seq 15001 second
   - seq 15002 third
2. Verify all are processed.

**Expected:**
- All 3 trades stored in `position_trades`
- Position snapshot correct regardless of arrival order
- WAC calculation uses sequence_num order (not arrival order)

---

### E2E-RESIL-005: Concurrent Trades for Same Position

**Steps:**
1. Publish 50 trades simultaneously for the same position key.
2. Wait for all to process.
3. Verify final position state.

**Expected:**
- Kafka partitioning ensures sequential processing per position_id
- Final netQuantity = sum of all signed quantities
- No race conditions or lost updates
- calculation_version reflects total recalculations

---

### E2E-RESIL-006: Very Large Position (100k+ Trades)

**Steps:**
1. Publish 100,000 trades for a single position key.
2. Monitor memory usage during calculation.
3. Verify final snapshot is correct.

**Expected:**
- Streaming cursor prevents memory overflow
- Memory usage stays constant (~100KB per calculation, not 10MB+)
- Correct final WAC and quantities
- No OOM errors

---

### E2E-RESIL-007: Position Key Uniqueness

**Steps:**
1. Publish trades for these distinct positions:
   - Book=A, Counterparty=B, Instrument=C → key "A#B#C"
   - Book=A, Counterparty=B, Instrument=D → key "A#B#D"
2. Verify separate position_keys entries.

**Expected:**
- Each combination gets its own position_id
- Snapshots are independent
- No cross-contamination of quantities

---

## 9. Data Integrity Tests

### E2E-DATA-001: Bitemporal Consistency

**Steps:**
1. Create position on Feb 5 (version 1).
2. Submit late trade affecting Feb 5 (creates version 2).
3. Query history for Feb 5.

**Expected:**
- Version 1: `superseded_at` is populated, `change_reason = "INITIAL"`
- Version 2: `superseded_at` is NULL (current), `change_reason = "LATE_TRADE"`
- `previous_net_quantity` on version 2 matches version 1's `net_quantity`
- `calculated_at` on version 2 > version 1

---

### E2E-DATA-002: Trade Date vs Settlement Date Independence

**Steps:**
1. Publish trade: tradeDate=Feb 5, settlementDate=Feb 7.
2. Publish different trade: tradeDate=Feb 7, settlementDate=Feb 9.
3. Query trade date snapshot for Feb 7.
4. Query settlement date snapshot for Feb 7.

**Expected:**
- Trade date snapshot for Feb 7 = only trade #2
- Settlement date snapshot for Feb 7 = only trade #1
- Different netQuantity, WAC, etc. (different trades contribute)

---

### E2E-DATA-003: Position Key Cache Accuracy

**Steps:**
1. Publish trades across multiple dates for one position.
2. Query `position_keys` table directly.
3. Verify `last_trade_date` and `last_settlement_date`.

**Expected:**
- `last_trade_date` = MAX of all trade dates for this position
- `last_settlement_date` = MAX of all settlement dates for this position
- Used GREATEST() for atomic concurrent updates

---

### E2E-DATA-004: Notional Calculation

**Steps:**
1. Buy 1000 @ $150 → notional = $150,000
2. Buy 500 @ $160 → notional += $80,000
3. Sell 200 @ $155 → notional += $31,000

**Expected:**
- `total_notional` = sum of |quantity × price| for all trades
- Consistent across trade and settlement snapshots

---

## 10. Test Infrastructure

### 10.1 Test Data Seeder

Build a CLI/script that seeds known test data:

```bash
# Seed basic positions for E2E testing
./scripts/seed-test-data.sh --scenario=basic       # 5 positions, 10 trades
./scripts/seed-test-data.sh --scenario=multi-day    # 3 positions, 30 days
./scripts/seed-test-data.sh --scenario=late-trades  # Pre-built cascade scenario
./scripts/seed-test-data.sh --scenario=performance  # 10k positions, 100k trades
./scripts/seed-test-data.sh --scenario=clean        # Truncate all tables
```

### 10.2 Kafka Test Producer

Utility to publish test trade events:

```kotlin
// TestTradePublisher.kt
fun publishTrade(
    book: String, counterparty: String, instrument: String,
    quantity: Long, price: BigDecimal,
    tradeDate: LocalDate, settlementDate: LocalDate,
    sequenceNum: Long
)
```

### 10.3 Assertion Helpers

```kotlin
// PositionAssertions.kt
fun assertPosition(key: String, date: LocalDate, dateBasis: DateBasis, expected: ExpectedPosition)
fun assertWac(key: String, date: LocalDate, expectedPrice: BigDecimal, tolerance: BigDecimal = "0.000001".toBigDecimal())
fun assertCascadeCompleted(key: String, fromDate: LocalDate, toDate: LocalDate)
fun assertHistoryVersions(key: String, date: LocalDate, expectedCount: Int)
```

### 10.4 UI Test Framework (Playwright)

```typescript
// playwright.config.ts
export default defineConfig({
  baseURL: 'http://localhost:5173',
  use: { browserName: 'chromium' },
  webServer: [
    { command: 'npm run dev', port: 5173, cwd: '../position-service-ui' },
    { command: './gradlew run', port: 8080, cwd: '../position-service' }
  ]
});
```

### 10.5 Environment Reset

```bash
# Between test runs
docker-compose down -v && docker-compose up -d
# Wait for healthy services
./scripts/wait-for-services.sh
```

---

## 11. Test Execution Matrix

### By Team

| Test Suite | Backend Team | UI Team | Both |
|---|---|---|---|
| E2E-FLOW-001 to 005 | Owns | Validates UI | Joint sign-off |
| E2E-CALC-001 to 010 | Owns | — | Backend sign-off |
| E2E-API-001 to 008 | Owns | Consumes | API contract freeze |
| E2E-UI-001 to 009 | Provides API | Owns | Joint sign-off |
| E2E-PERF-001 to 008 | Owns 001-007 | Owns 008 | Joint sign-off |
| E2E-RESIL-001 to 007 | Owns | — | Backend sign-off |
| E2E-DATA-001 to 004 | Owns | — | Backend sign-off |

### By Priority

| Priority | Tests | Gate |
|---|---|---|
| **P0 — Must Pass** | FLOW-001, FLOW-002, CALC-001 to 004, API-001 to 003, UI-001 | Release blocker |
| **P1 — Should Pass** | FLOW-003 to 005, CALC-005 to 009, API-004 to 006, UI-002 to 006, DATA-001 to 004 | Release warning |
| **P2 — Nice to Have** | CALC-010, API-007 to 008, UI-007 to 009, PERF-*, RESIL-* | Post-release OK |

### Automation Timeline

| Phase | Automated | Manual |
|---|---|---|
| **Week 1** | API contract tests (E2E-API-*), WAC calc tests (E2E-CALC-*) | UI smoke tests |
| **Week 2** | Full pipeline tests (E2E-FLOW-*), data integrity (E2E-DATA-*) | UI interaction tests |
| **Week 3** | Playwright UI tests (E2E-UI-*) | Performance baseline |
| **Week 4** | Performance tests (E2E-PERF-*), resilience (E2E-RESIL-*) | Exploratory testing |

---

## 12. Exit Criteria

All of the following must be met before release:

- [ ] All P0 tests passing in CI
- [ ] All P1 tests passing (or documented exceptions)
- [ ] Performance benchmarks met (see Section 7)
- [ ] No critical/high severity bugs open
- [ ] API contract frozen and documented
- [ ] UI renders correct data for all date bases
- [ ] Late trade cascade verified for both trade and settlement dates
- [ ] WAC correctness validated against all 4 rules
- [ ] Bitemporal history queryable through API and UI
- [ ] Zero data loss under normal operation confirmed

---

**Document End**
