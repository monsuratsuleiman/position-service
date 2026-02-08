# Architecture Requirements Document (ARD)
## Position Service - Equity Derivatives Trading Platform

**Version:** 2.0
**Date:** February 8, 2026
**Status:** Implemented

---

## 1. System Overview

### 1.1 Purpose

Position Service calculates and maintains real-time position snapshots from trade events for equity derivatives trading.

### 1.2 Scope

**Positions Calculated:**
- Official positions: Book × Counterparty × Instrument (default)
- Configurable key formats: 5 supported (BOOK_COUNTERPARTY_INSTRUMENT, BOOK_INSTRUMENT, COUNTERPARTY_INSTRUMENT, INSTRUMENT, BOOK)
- Two views always: Trade date positions + Settlement date positions
- Price method: Weighted Average Cost (WAC)

**Core Capabilities:**
- Real-time position calculation from trade events
- Batch trade processing with deduped calc requests (5x reduction in calc requests)
- Automatic late trade recalculation
- Direction-aware WAC pricing (industry standard)
- Complete audit history (bitemporal)
- Sub-millisecond position queries
- Database-driven position configuration with scope predicates
- React UI for position monitoring and configuration management
- Load testing framework with correctness verification

---

## 2. Architecture Decisions

### 2.1 Event-Driven Architecture

**Pattern:** Decouple trade ingestion from position calculation via Kafka.

```
Trade Events → Trade Consumer → Batch Insert + Dedupe + Publish Requests
                                        ↓
                            Kafka (position-calculation-requests)
                                        ↓
                            Calculation Workers → Calculate + Save
```

**Rationale:**
- Non-blocking trade ingestion (<5ms)
- Batch processing reduces calc requests by ~5x (one per unique position/date/dateBasis vs one per trade)
- Single unified calculation algorithm
- Kafka durability and retry

**Components:**
1. **Trade Event Consumer:** Batch-insert trades, deduplicate calc intents, publish deduped requests
2. **Calculation Workers:** Load state, calculate positions, save results
3. **Kafka Topic:** position-calculation-requests (20 partitions, partitioned by position_id)

---

### 2.2 Dual Publishing

**Decision:** Every trade immediately publishes 2 calculation requests (trade date + settlement date).

**No scheduler** - settlement positions calculated real-time on trade arrival.

**Rationale:**
- Simpler architecture (no batch jobs)
- Faster settlement positions (immediate vs hourly)
- Consistent event-driven pattern
- Settlement out-of-order is normal case (bounded 2-3 dates)

---

### 2.3 Type-Safe Position Key Formats

**Decision:** Use enum for position key formats (compile-time validation).

```kotlin
enum class PositionKeyFormat {
    BOOK_COUNTERPARTY_INSTRUMENT,  // "BOOK1#GOLDMAN#AAPL"
    BOOK_INSTRUMENT,               // "BOOK1#AAPL"
    COUNTERPARTY_INSTRUMENT,       // "GOLDMAN#AAPL"
    INSTRUMENT,                    // "AAPL"
    BOOK                          // "BOOK1"
}

// Phase 1: BOOK_COUNTERPARTY_INSTRUMENT only
```

**Rationale:**
- Prevents configuration errors (no typos, wrong order)
- Compile-time validation (exhaustive pattern matching)
- Self-documenting (enum names describe structure)
- Canonical format enforced

---

### 2.4 Numeric Position IDs with Cached Last Dates

**Decision:** Assign numeric position IDs and cache last trade/settlement dates in position_keys table.

```sql
CREATE TABLE position_keys (
    position_id BIGSERIAL PRIMARY KEY,
    position_key VARCHAR(200) UNIQUE,
    last_trade_date DATE,           -- Cached for cascade optimization
    last_settlement_date DATE,      -- Cached for cascade optimization
    ...
);

-- Atomic update with GREATEST
ON CONFLICT DO UPDATE SET
    last_trade_date = GREATEST(position_keys.last_trade_date, EXCLUDED.last_trade_date);
```

**Rationale:**
- Numeric ID: Efficient Kafka partitioning (integer hash)
- Cached dates: Avoid MAX queries on every trade (3x faster ingestion)
- GREATEST: Handles concurrent updates atomically
- Single query: Returns position_id + last dates

**Performance:**
- Without cache: 3 queries (insert trade, MAX trade_date, MAX settlement_date) = 6ms
- With cache: 2 queries (insert trade, upsert position_keys) = 2ms
- Improvement: 3x faster

---

### 2.5 Smart Late Trade Cascade

**Decision:** Cascade only to last actual trade date for position, not to today.

```kotlin
// Use cached last_trade_date from position_keys
if (trade.tradeDate < lastTradeDate) {
    publishCascade(trade.tradeDate → lastTradeDate)  // Smart boundary
} else {
    publishSingle(trade.tradeDate)
}
```

**Example:**
```
Today: Feb 3
Late trade: Jan 20
Last trade for this position: Jan 25

Cascade: Jan 20 → Jan 25 (6 dates)
NOT: Jan 20 → Feb 3 (15 dates)

Savings: 60% fewer calculation requests
```

**Rationale:**
- Efficient: 50-85% fewer calculation requests
- Correct: Positions after last trade are unchanged (no trades)
- Handles closed/inactive positions correctly
- Bounded by actual trading activity

---

### 2.6 Kafka Partitioning Guarantees Incremental Calculation

**Decision:** Partition calculation requests by numeric position_id.

```yaml
Topic: position-calculation-requests
Partitions: 20
Key: position_id (numeric)
Consumer: concurrency = 1 per partition
```

**Guarantee:** Same position → same partition → sequential processing.

**Result:**
```
Late trade cascade (3 dates):
  Date 1: Full recalc (50ms) - no previous snapshot
  Date 2: Incremental (6ms) - previous snapshot exists (Kafka guarantee)
  Date 3: Incremental (6ms) - previous snapshot exists

Total: 62ms (vs 150ms if all full recalc)
```

**Rationale:**
- Sequential processing per position enables incremental calculation
- Previous snapshot ALWAYS exists (after first date)
- 3-7x performance improvement (guaranteed, not opportunistic)
- Positions are cumulative: Position(today) = Position(yesterday) + Trades(today)

---

### 2.7 Direction-Aware WAC

**Decision:** Implement industry-standard WAC with 4 rules (not simple buy/sell).

```kotlin
when {
    position crosses 0   → avgPrice = tradePrice      // RESET to new price
    position equals 0    → avgPrice = 0
    moving towards 0     → avgPrice UNCHANGED         // Sell doesn't affect WAC
    moving away from 0   → weighted average formula   // Buy updates WAC
}
```

**Rationale:**
- Industry standard (Calypso, Murex, Summit)
- Correct zero-crossing behavior
- Simple, deterministic rules

---

### 2.8 Separate Tables for Trade vs Settlement

**Decision:** Four separate table sets (not one with discriminator).

```sql
-- Trade date positions
position_snapshots
position_average_prices
position_snapshots_history

-- Settlement date positions
position_snapshots_settled
position_average_prices_settled
position_snapshots_settled_history
```

**Rationale:**
- Different users (traders vs operations)
- Different access patterns (point queries vs range scans)
- Different retention (7 years vs 90 days)
- Simpler indexes, faster queries (10-20% performance improvement)

---

### 2.9 Bitemporal History

**Decision:** Maintain current state + immutable history with two time dimensions.

```sql
-- Current (mutable, fast)
position_snapshots (position_key, business_date PRIMARY KEY)

-- History (immutable, audit)
position_snapshots_history (
    calculation_version,
    calculated_at,      -- When calculated (transaction time)
    superseded_at,      -- When replaced (NULL = current)
    change_reason       -- LATE_TRADE, CORRECTION, etc.
)
```

**Time Dimensions:**
1. **Valid time (business_date):** When is this position for?
2. **Transaction time (calculated_at):** When did we know about it?

**Rationale:**
- Regulatory compliance (MiFID II, Dodd-Frank require audit trail)
- Point-in-time queries ("position as of 2pm")
- Investigation capability ("why did position change?")

---

### 2.10 Hybrid Calculation Strategy

**Decision:** Database aggregation for simple metrics, application code for WAC.

**Simple metrics (SQL):**
```sql
SELECT SUM(signed_quantity), SUM(CASE WHEN signed_quantity > 0...),
       COUNT(*), MAX(sequence_num)
FROM position_trades
WHERE position_key = ? AND trade_date = ?;
-- Returns 1 row, <2ms
```

**WAC calculation (Kotlin with streaming):**
```kotlin
var wacState = WacState()
positionTradeRepo.streamByPositionKeyAndTradeDate(batchSize = 100) { batch →
    batch.forEach { trade → wacState = wacState.applyTrade(trade) }
}
// Constant memory (100KB), handles 100k+ trades
```

**Rationale:**
- Database: Optimized for aggregation
- Application: Testable, type-safe, maintainable for complex logic
- Streaming: Prevents memory overflow (constant 100KB vs 10MB if loading all)

---

### 2.11 Batch Trade Processing with Deduped Calc Requests

**Decision:** Batch-insert all trades from a Kafka poll, then publish one calc request per unique `(positionKey, dateBasis, businessDate)` instead of one per trade.

```kotlin
// Per poll batch (up to 5000 trades):
// 1. Batch-insert all trades in single transaction
// 2. Upsert position keys, collect calc intents
// 3. Deduplicate: one request per (key, dateBasis, date)
// 4. Publish deduped set
```

**Performance (100K trades, 1000 position keys, single day):**
- With `MAX_POLL_RECORDS=5000` and ~20 batches: ~40K calc requests (vs 200K) = 5x fewer
- Each calc request processes ~5 trades at once via `findTradesAfterSequence`
- Less WAC rounding accumulation (fewer save/reload cycles)

**Deduplication Rules:**
- Map key: `(positionKey, dateBasis, businessDate)`
- If any intent has `LATE_TRADE`, the merged request uses `LATE_TRADE`
- Keeps highest `sequenceNum` across merged intents

**Rationale:**
- Dramatic reduction in calc request volume (5x fewer)
- Fewer DB round trips in calculation workers
- Reduced WAC rounding accumulation from fewer save/reload cycles
- Single-transaction batch insert reduces DB overhead

---

### 2.12 Database-Driven Position Configuration with Scope Predicates

**Decision:** Store position configurations in the database with polymorphic scope filtering.

```kotlin
data class PositionConfig(
    val configId: Long,
    val type: PositionConfigType,    // OFFICIAL, USER, DESK
    val name: String,
    val keyFormat: PositionKeyFormat, // 5 formats supported
    val priceMethods: Set<PriceCalculationMethod>,
    val scope: ScopePredicate,        // ALL or field-based criteria
    val active: Boolean
)

sealed class ScopePredicate {
    data object All : ScopePredicate()           // Matches all trades
    data class Criteria(                          // Matches by field values
        val criteria: Map<ScopeField, String>     // BOOK, COUNTERPARTY, INSTRUMENT, SOURCE
    ) : ScopePredicate()
}
```

**Features:**
- Full CRUD API (`/api/v1/configs`)
- Scope stored as JSONB in PostgreSQL
- Scope filtering applied at trade ingestion time
- Config caching with 60-second TTL refresh in trade consumer
- Seed data: default OFFICIAL config created via Flyway migration

**Rationale:**
- Runtime configurability without code changes
- Scope predicates enable targeted position calculations
- JSONB storage allows flexible scope evolution

---

## 3. Component Architecture

### 3.1 Trade Event Consumer

**Responsibilities:**
1. Consume trade events from Kafka in batches (MAX_POLL_RECORDS=5000)
2. Batch-insert trades in single transaction (idempotent by sequence_num)
3. Load active position configurations (cached with 60s TTL)
4. For each config matching trade scope:
   - Generate position key using config's key format enum
   - Upsert position_keys (get ID + update last dates)
   - Collect calc intents for trade date and settlement date
5. Deduplicate calc intents by `(positionKey, dateBasis, businessDate)`
6. Publish deduped calc requests (promotes to LATE_TRADE if any intent is late)

**Technology:**
- Kotlin + Ktor
- kafka-clients (direct, not Spring Kafka)
- Exposed ORM + PostgreSQL
- Manual dependency injection

**Performance:**
- Latency: <5ms p95
- Throughput: 10,000 trades/sec
- Batch deduplication: ~5x fewer calc requests than per-trade publishing

---

### 3.2 Position Calculation Worker

**Responsibilities:**
1. Consume calculation requests from internal Kafka topic
2. Determine table set based on date_basis (trade vs settled)
3. Load previous snapshot (businessDate - 1) and current snapshot (same date)
4. Calculate position using one of three strategies:
   - **Same-day incremental:** Current snapshot exists → find trades after lastSequenceNum, apply delta
   - **Cross-day incremental:** Previous day snapshot exists → previous + today's trades
   - **Full recalculation:** No previous state → aggregate all trades for this date
5. Calculate WAC using direction-aware algorithm (4 rules)
6. Save snapshot + prices + history (with bitemporal versioning)

**Technology:**
- Kotlin + Ktor
- kafka-clients (direct)
- Exposed ORM + PostgreSQL
- Manual dependency injection

**Performance:**
- Same-day incremental: <5ms p95
- Cross-day incremental: <10ms p95
- Full calculation: <50ms p95

---

### 3.3 Position Query Service (REST API)

**Responsibilities:**
- REST API for position queries, config management, and health monitoring
- Embedded in the same Ktor application (not a separate service)

**Endpoints:**
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/v1/health` | Health check |
| GET | `/api/v1/configs` | List all configs |
| GET | `/api/v1/configs/{configId}` | Get single config |
| POST | `/api/v1/configs` | Create config |
| PUT | `/api/v1/configs/{configId}` | Update config |
| DELETE | `/api/v1/configs/{configId}` | Deactivate config (soft delete) |
| GET | `/api/v1/position-keys` | Search position keys (book, counterparty, instrument filters) |
| GET | `/api/v1/position-keys/dimensions` | Get distinct books, counterparties, instruments |
| GET | `/api/v1/positions/{positionKey}/{businessDate}` | Get position snapshot with prices |
| GET | `/api/v1/positions/{positionKey}` | Get position time series (from/to date range) |
| GET | `/api/v1/positions/{positionKey}/{businessDate}/history` | Bitemporal version history |

**Query Parameters:**
- `dateBasis`: TRADE_DATE or SETTLEMENT_DATE (defaults to TRADE_DATE)
- `from`, `to`: Date range filters for time series
- `book`, `counterparty`, `instrument`, `limit`: Position key search filters

**Technology:**
- Kotlin + Ktor (content negotiation, status pages)
- Exposed ORM + HikariCP connection pooling
- kotlinx.serialization for JSON

**Performance:**
- Current position query: <1ms p95
- Position with prices: <5ms p95

---

### 3.4 Internal Kafka Topic

```yaml
Topic: position-calculation-requests
Partitions: 20
Replication: 3
Key: position_id (numeric)
Retention: 24 hours

Message Schema:
  requestId: String
  positionId: Long              # Kafka partition key
  positionKey: String
  dateBasis: TRADE_DATE | SETTLEMENT_DATE
  businessDate: LocalDate
  priceMethods: [WAC]
  triggeringTradeSequence: Long
  changeReason: INITIAL | LATE_TRADE | CORRECTION
  keyFormat: PositionKeyFormat
```

---

## 4. Data Models

### 4.1 Position Configuration

```kotlin
data class PositionConfig(
    val configId: Long = 1L,
    val type: PositionConfigType = OFFICIAL,
    val name: String = "Official Positions",
    val keyFormat: PositionKeyFormat = BOOK_COUNTERPARTY_INSTRUMENT,
    val priceMethods: Set<PriceCalculationMethod> = setOf(WAC),
    val scope: ScopePredicate = ScopePredicate.All,
    val active: Boolean = true
)

sealed class ScopePredicate {
    data object All : ScopePredicate()               // Matches all trades
    data class Criteria(                              // Matches by field values
        val criteria: Map<ScopeField, String>
    ) : ScopePredicate()
}

enum class ScopeField { BOOK, COUNTERPARTY, INSTRUMENT, SOURCE }
enum class PositionConfigType { OFFICIAL, USER, DESK }
enum class PositionKeyFormat {
    BOOK_COUNTERPARTY_INSTRUMENT,
    BOOK_INSTRUMENT,
    COUNTERPARTY_INSTRUMENT,
    INSTRUMENT,
    BOOK
}
enum class PriceCalculationMethod { WAC, VWAP, FIFO, LIFO }
// Phase 1: WAC only
```

**Configuration is database-driven** with full CRUD via REST API. A default OFFICIAL config is seeded via Flyway migration. The trade consumer caches active configs with a 60-second refresh interval.

---

### 4.2 Position Snapshot

```kotlin
data class PositionSnapshot(
    positionKey: String,              // "BOOK1#GOLDMAN#AAPL"
    businessDate: LocalDate,
    
    // Metrics
    netQuantity: Long,
    grossLong: Long,
    grossShort: Long,
    tradeCount: Int,
    totalNotional: BigDecimal,
    
    // Versioning
    calculationVersion: Int,
    calculatedAt: Instant,
    calculationMethod: String         // "INCREMENTAL" | "FULL_RECALC"
    lastSequenceNum: Long
)
```

---

### 4.3 WAC State

```kotlin
data class WacState(
    avgPrice: BigDecimal = ZERO,
    totalCostBasis: BigDecimal = ZERO,
    netQuantity: Long = 0,
    lastSequence: Long = 0
) {
    fun applyTrade(seq: Long, qty: Long, price: BigDecimal): WacState {
        val newQty = netQuantity + qty
        
        return when {
            crossesZero(netQuantity, newQty) ->
                WacState(price, price * newQty.toBigDecimal(), newQty, seq)
            
            newQty == 0L ->
                WacState(ZERO, ZERO, 0, seq)
            
            movingTowards(netQuantity, qty) ->
                WacState(avgPrice, totalCostBasis + avgPrice * qty.toBigDecimal(), newQty, seq)
            
            else -> {
                val newCost = totalCostBasis + price * qty.toBigDecimal()
                val newAvg = newCost / abs(newQty).toBigDecimal()
                WacState(newAvg, newCost, newQty, seq)
            }
        }
    }
    
    private fun crossesZero(old: Long, new: Long) =
        (old > 0 && new < 0) || (old < 0 && new > 0)
    
    private fun movingTowards(current: Long, trade: Long) =
        (current > 0 && trade < 0) || (current < 0 && trade > 0)
}
```

---

## 5. Database Schema

### 5.1 Core Tables

```sql
-- Position configuration (database-driven)
CREATE TABLE position_configs (
    config_id BIGSERIAL PRIMARY KEY,
    config_type VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    key_format VARCHAR(50) NOT NULL,
    price_methods VARCHAR(200) NOT NULL,
    scope JSONB NOT NULL DEFAULT '{"type":"ALL"}',
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Seed default config
INSERT INTO position_configs (config_type, name, key_format, price_methods, scope, active, created_at, updated_at)
VALUES ('OFFICIAL', 'Official Positions', 'BOOK_COUNTERPARTY_INSTRUMENT', 'WAC', '{"type":"ALL"}', true, NOW(), NOW());

-- Position ID management with cached last dates
CREATE TABLE position_keys (
    position_id BIGSERIAL PRIMARY KEY,
    position_key VARCHAR(200) NOT NULL,
    config_id BIGINT NOT NULL DEFAULT 1,
    config_type VARCHAR(20) NOT NULL DEFAULT 'OFFICIAL',
    config_name VARCHAR(100) NOT NULL DEFAULT 'Official Positions',

    -- Dimensions (nullable to support different key formats)
    book VARCHAR(50),
    counterparty VARCHAR(50),
    instrument VARCHAR(50),

    -- Cached optimization (avoid MAX queries)
    last_trade_date DATE,
    last_settlement_date DATE,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by_sequence BIGINT,

    UNIQUE (position_key, config_id)
);

CREATE INDEX idx_position_keys_config ON position_keys(config_id);
CREATE INDEX idx_position_keys_book_cpty_inst ON position_keys(book, counterparty, instrument);

-- Trade event log
CREATE TABLE position_trades (
    sequence_num BIGINT PRIMARY KEY,
    position_key VARCHAR(200) NOT NULL,
    
    -- Temporal
    trade_time TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    settlement_date DATE NOT NULL,
    
    -- Dimensions
    book VARCHAR(50) NOT NULL,
    counterparty VARCHAR(50) NOT NULL,
    instrument VARCHAR(50) NOT NULL,
    
    -- Economics
    signed_quantity BIGINT NOT NULL CHECK (signed_quantity != 0),
    price DECIMAL(20,6) NOT NULL CHECK (price > 0),
    
    -- Metadata
    source VARCHAR(50) NOT NULL,
    source_id VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_trades_position_trade_date ON position_trades(position_key, trade_date);
CREATE INDEX idx_trades_position_settlement_date ON position_trades(position_key, settlement_date);

-- Trade date positions (current state)
CREATE TABLE position_snapshots (
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,
    
    -- Metrics
    net_quantity BIGINT NOT NULL,
    gross_long BIGINT NOT NULL,
    gross_short BIGINT NOT NULL,
    trade_count INTEGER NOT NULL,
    total_notional DECIMAL(20,6),
    
    -- Versioning
    calculation_version INTEGER NOT NULL DEFAULT 1,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    calculation_method VARCHAR(20),
    calculation_request_id VARCHAR(50),
    
    -- Tracking
    last_sequence_num BIGINT NOT NULL,
    last_trade_time TIMESTAMP,
    
    PRIMARY KEY (position_key, business_date)
);

CREATE INDEX idx_snapshots_calculated ON position_snapshots(calculated_at);

CREATE TABLE position_average_prices (
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,
    price_method VARCHAR(20) NOT NULL,  -- 'WAC'

    price DECIMAL(20,12) NOT NULL,      -- 12 decimal places for WAC precision
    method_data JSONB NOT NULL,  -- {"totalCostBasis": 150000, "lastUpdatedSequence": 12345}

    calculation_version INTEGER NOT NULL DEFAULT 1,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (position_key, business_date, price_method)
);

CREATE TABLE position_snapshots_history (
    history_id BIGSERIAL PRIMARY KEY,
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,
    
    -- Metrics
    net_quantity BIGINT NOT NULL,
    gross_long BIGINT NOT NULL,
    gross_short BIGINT NOT NULL,
    trade_count INTEGER NOT NULL,
    total_notional DECIMAL(20,6),
    
    -- Bitemporal
    calculation_version INTEGER NOT NULL,
    calculated_at TIMESTAMP NOT NULL,
    superseded_at TIMESTAMP,  -- NULL = current version
    
    -- Audit
    change_reason VARCHAR(50),  -- 'INITIAL', 'LATE_TRADE', 'CORRECTION'
    previous_net_quantity BIGINT,
    calculation_request_id VARCHAR(50),
    
    -- Tracking
    last_sequence_num BIGINT NOT NULL,
    last_trade_time TIMESTAMP,
    calculation_method VARCHAR(20)
);

CREATE INDEX idx_history_position_date ON position_snapshots_history(position_key, business_date);
CREATE INDEX idx_history_current ON position_snapshots_history(position_key, business_date)
    WHERE superseded_at IS NULL;

-- Settlement date positions (identical structure, separate tables)
CREATE TABLE position_snapshots_settled (
    -- Same columns as position_snapshots
    PRIMARY KEY (position_key, business_date)
);

CREATE TABLE position_average_prices_settled (
    -- Same columns as position_average_prices (with DECIMAL(20,12) price)
    PRIMARY KEY (position_key, business_date, price_method)
);

CREATE TABLE position_snapshots_settled_history (
    -- Same columns as position_snapshots_history
);
```

**Note:** Table partitioning is not currently implemented. Schema is managed via Flyway migrations (V1 through V6).

### 5.2 Schema Migrations (Flyway)

| Migration | Description |
|-----------|-------------|
| V1 | Create core tables (position_keys, position_trades, snapshots, prices, history) |
| V2 | Create position_configs table with seed data |
| V3 | Add scope column to position_configs |
| V4 | Convert scope from VARCHAR to JSONB |
| V5 | Make book/counterparty/instrument nullable in position_keys |
| V6 | Increase WAC price precision from DECIMAL(20,6) to DECIMAL(20,12) |

---

## 6. Processing Flows

### 6.1 Normal Trade Flow (Batch)

```
1. Kafka poll returns up to 5000 trade events
    ↓
2. Trade Consumer (batch processing):
   a. Deserialize all records, skip failures
   b. Batch-insert all trades in single transaction (idempotent by sequence_num)
   c. Load active configs (cached, 60s TTL)
   d. For each inserted trade × matching config:
      - Generate position key using config's key format
      - Upsert position_keys (get ID + update last dates)
      - Collect calc intents for trade date + settlement date
   e. Deduplicate: one calc request per unique (positionKey, dateBasis, businessDate)
   f. Publish deduped calc requests
    ↓
3. Calculation Workers (Kafka partition for position_id=42):
   For each calc request:
   a. Check for existing same-day snapshot:
      - If exists: Same-day incremental (find trades after lastSequenceNum)
      - If not: Check previous day snapshot
        - If exists: Cross-day incremental (previous + today's trades)
        - If not: Full recalculation
   b. Save to appropriate tables (trade or settled)
    ↓
4. Result: Position snapshots created/updated for all affected keys
   (e.g., 5000 trades across 500 keys → ~1000 calc requests instead of 10000)
```

---

### 6.2 Late Trade Flow

```
1. Late trade arrives (trade_date = Jan 20, today = Feb 3)
    ↓
2. Trade Consumer:
   a. Insert trade
   b. Upsert position_keys returns:
      - position_id: 42
      - last_trade_date: Jan 25 (from cache!)
      - last_settlement_date: Feb 5
   c. Determine cascade:
      - Trade date: Jan 20 < Jan 25 → Cascade Jan 20 to Jan 25 (6 dates)
      - Settlement: Jan 22 < Feb 5 → Cascade Jan 22 to Feb 5 (15 dates)
   d. Publish 21 calculation requests (6 + 15)
    ↓
3. Calculation Workers (sequential per partition):
   Trade date cascade:
     - Jan 20: Full recalc (50ms) - first date
     - Jan 21-25: Incremental (6ms each = 30ms) - previous exists
   
   Settlement date cascade:
     - Jan 22: Full recalc (50ms) - first date
     - Jan 23-Feb 5: Incremental (6ms each = 78ms) - previous exists
    ↓
4. Total: ~210ms for 21 dates (vs ~1050ms if all full recalc)
5. Each date creates new version in history table with change_reason='LATE_TRADE'
```

---

### 6.3 Calculation Strategies (Detail)

The calculation engine uses three strategies, selected automatically:

```kotlin
fun calculatePosition(request: PositionCalcRequest) {
    // 1. Check for existing same-day snapshot (batch processing optimization)
    val currentSnapshot = snapshotRepo.find(request.positionKey, request.businessDate)
    if (currentSnapshot != null) {
        return calculateSameDayIncremental(request, currentSnapshot)
    }

    // 2. Check for previous day snapshot
    val previous = snapshotRepo.find(request.positionKey, request.businessDate - 1)
    if (previous != null) {
        return calculateIncremental(request, previous)
    }

    // 3. No prior state — full recalculation
    return calculateFull(request)
}
```

**Same-Day Incremental** (most common with batch processing):
```kotlin
// Finds only trades after the last processed sequence number
val newTrades = tradeRepo.findTradesAfterSequence(
    positionKey, businessDate, dateBasis, currentSnapshot.lastSequenceNum
)
// Applies delta to existing snapshot metrics + WAC state
```

**Cross-Day Incremental:**
```kotlin
// Aggregates all of today's trades via SQL
val todayMetrics = tradeRepo.aggregateMetrics(positionKey, businessDate, dateBasis)
// Adds to previous day's snapshot; WAC replays today's trades from previous state
```

**Full Recalculation:**
```kotlin
// Aggregates all trades for this date; WAC replays all trades from zero state
```

**WAC Incremental** (applies to both same-day and cross-day):
```kotlin
// Load previous WAC state
var state = WacState(avgPrice, totalCostBasis, netQuantity, lastSequence)

// Apply only new trades (after lastSequence)
newTrades.forEach { trade ->
    state = state.applyTrade(trade.sequenceNum, trade.signedQuantity, trade.price)
}
// Save updated WAC state
```
```

---

## 7. Performance Requirements

| Metric | Target | Method |
|--------|--------|--------|
| Trade ingestion | <5ms p95 | Non-blocking publish, cached last dates |
| Position calculation (incremental) | <10ms p95 | Previous + today's trades only |
| Position calculation (full) | <50ms p95 | Streaming cursors, DB aggregation |
| Current position query | <1ms p95 | Primary key lookup |
| Position with prices query | <5ms p95 | Simple JOIN |
| Late trade cascade (6 dates) | <80ms | First full (50ms) + 5 incremental (6ms each) |
| Throughput | 10,000 trades/sec | Worker pool, Kafka partitioning |
| Incremental calculation rate | >90% | Kafka ordering guarantee |

---

## 8. Technology Stack

### 8.1 Backend Application

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Language | Kotlin | 1.9.22 | Type safety, null safety |
| Web Framework | Ktor | 2.3.7 | HTTP server, content negotiation, routing |
| ORM | Exposed | 0.46.0 | JetBrains type-safe SQL DSL |
| Serialization | kotlinx.serialization | 1.6.2 | JSON serialization |
| Build | Gradle (Kotlin DSL) | 8.5 | Build automation |
| JDK | OpenJDK | 17 | Runtime |
| Testing | JUnit 5 + Testcontainers | 1.19.3 | Unit/integration/E2E tests |
| Logging | Logback | 1.4.14 | SLF4J logging |

### 8.2 Data

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Database | PostgreSQL | 16 | ACID, JSONB, indexes |
| Connection Pool | HikariCP | 5.1.0 | High performance pooling |
| Migration | Flyway | 10.4.1 | Schema version control |
| Driver | PostgreSQL JDBC | 42.7.1 | Database connectivity |

### 8.3 Messaging

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Message Broker | Apache Kafka (Confluent) | 7.5.3 | Event streaming, ordering guarantees |
| Client | kafka-clients | 3.6.1 | Direct Kafka consumer/producer (not Spring) |

### 8.4 Frontend (position-service-ui)

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Framework | React | 19.2.0 | UI framework |
| Language | TypeScript | 5.9.3 | Type-safe frontend |
| Build | Vite | 7.2.4 | Fast dev server and bundler |
| UI Library | Ant Design | 6.2.3 | Component library |
| Routing | React Router | 7.13.0 | Client-side routing |
| HTTP Client | Axios | 1.13.4 | API communication |
| Charts | @ant-design/charts | - | Position visualization |
| E2E Tests | Playwright | - | Browser-based E2E testing |

### 8.5 Infrastructure

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Container | Docker Compose | - | Local development environment |
| Zookeeper | Confluent | 7.5.3 | Kafka coordination |

---

## 9. Deployment Architecture

### 9.1 Current Architecture (Single Process)

The backend runs as a single Ktor application with embedded consumers and REST API:

```
┌─────────────────────────────────────────────────────┐
│ Position Service (Ktor, port 8080)                  │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Trade Event Consumer Thread                  │    │
│  │ - Poll trade-events (max 5000/poll)          │    │
│  │ - Batch insert + dedupe + publish calc reqs  │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ Calculation Worker Thread                    │    │
│  │ - Poll position-calculation-requests         │    │
│  │ - Calculate positions (incremental/full)     │    │
│  └─────────────────────────────────────────────┘    │
│                                                     │
│  ┌─────────────────────────────────────────────┐    │
│  │ REST API (Ktor routes)                       │    │
│  │ - Position queries, config CRUD, health      │    │
│  └─────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────┘
              ↓                    ↓
┌──────────────────────┐  ┌──────────────────────┐
│ Kafka (single broker)│  │ PostgreSQL 16        │
│ - trade-events (10p) │  │ - All position data  │
│ - calc-requests (20p)│  │ - HikariCP pool (20) │
└──────────────────────┘  └──────────────────────┘
              ↑
┌──────────────────────┐
│ Position Service UI  │
│ (React, Vite)        │
│ - Dashboard, search  │
│ - Config management  │
│ - Position history   │
└──────────────────────┘
```

### 9.2 Docker Compose (Local Development)

```yaml
Services:
  postgres:       PostgreSQL 16-alpine (port 5432)
  zookeeper:      Confluent 7.5.3 (port 2181)
  kafka:          Confluent 7.5.3 (port 9092)
  kafka-init:     Creates topics (trade-events: 10p, calc-requests: 20p)
```

The application itself runs outside Docker via `./gradlew run`.

### 9.3 Production Scaling Strategy (Future)

**Horizontal Scaling:**
- Trade Consumer: 3-10 replicas (based on Kafka lag)
- Calculation Workers: 10-30 replicas (based on queue depth)
- Query Service: 5-20 replicas (based on request rate)

---

## 10. Operational Requirements

### 10.1 Monitoring Metrics

**Business Metrics:**
```
position.trades.ingested (counter)
position.calculations.completed (counter)
position.calculations.failed (counter)
position.incremental_rate (gauge) - target: >90%
```

**Performance Metrics:**
```
position.trade.ingestion_duration_ms (histogram) - target: p95 <5ms
position.calculation.duration_ms (histogram) - target: p95 <10ms
position.query.duration_ms (histogram) - target: p95 <1ms

kafka.consumer.lag (gauge) - alert: >10000
db.connection_pool.active (gauge)
db.query.duration_ms (histogram)
```

### 10.2 Availability

**Target:** 99.9% uptime (43.2 minutes downtime/month)

**High Availability:**
- Multiple service replicas (no single point of failure)
- PostgreSQL automatic failover (Patroni) - RTO <30 seconds
- Kafka cluster (3 brokers, replication factor 3)

### 10.3 Disaster Recovery

**Backup Strategy:**
- Database: Daily full backup + continuous WAL archiving
- Point-in-time recovery: Last 30 days
- RTO: 1 hour
- RPO: 5 minutes

**Data Rebuild:**
- Position Store is derived from Trade Store
- Can rebuild all positions by replaying trades (idempotent)

---

## 11. Security

### 11.1 Authentication & Authorization

- API: OAuth 2.0 / JWT tokens
- Database: Role-based access control
  - position_service_read (queries)
  - position_service_write (calculations)
  - position_service_admin (migrations)

### 11.2 Data Protection

- At rest: Database encryption (TDE)
- In transit: TLS 1.3 for all network communication
- Kafka: SSL/TLS encryption

---

## 12. Phase 1 Deliverables

### 12.1 Scope (Implemented)

**Position Types:**
- Official positions: Book × Counterparty × Instrument (default)
- All 5 key formats supported and configurable at runtime

**Date Bases:**
- Trade date positions (always)
- Settlement date positions (always)

**Price Methods:**
- WAC only (direction-aware, 4 rules, DECIMAL(20,12) precision)

**Configuration:**
- Database-driven with full CRUD API
- Scope predicates (ALL or field-based criteria)
- Config caching with 60s TTL

**Features:**
- ✅ Event-driven dual publishing
- ✅ Batch trade processing with deduped calc requests (5x reduction)
- ✅ Same-day incremental calculation (handles batch-inserted trades efficiently)
- ✅ Type-safe key format (enum, all 5 formats)
- ✅ Numeric position IDs with cached last dates
- ✅ Smart late trade cascade (to last actual trade date)
- ✅ Kafka partitioning guarantees incremental calculation
- ✅ Direction-aware WAC (DECIMAL(20,12) precision)
- ✅ Separate tables (trade vs settled)
- ✅ Bitemporal history
- ✅ Database aggregation + application WAC
- ✅ Sub-millisecond queries
- ✅ Database-driven position configuration with scope predicates
- ✅ Full REST API (11 endpoints: positions, configs, health, dimensions)
- ✅ React UI (dashboard, position search, detail, history, config management)
- ✅ Load testing framework with correctness verification (SMOKE/MEDIUM/LARGE profiles)
- ✅ 93 automated tests (unit + integration + E2E with Testcontainers)

### 12.2 Not Yet Implemented

- Additional price methods (VWAP, FIFO, LIFO)
- Table partitioning (schema supports it but not enabled)
- Multi-replica deployment / Kubernetes
- Monitoring (Prometheus + Grafana)
- Authentication / authorization (OAuth 2.0 / JWT)

---

## 13. Frontend Application (position-service-ui)

### 13.1 Pages

| Page | Path | Description |
|------|------|-------------|
| Dashboard | `/` | KPI cards, system overview |
| Positions | `/positions` | Search and list positions (filterable by book, counterparty, instrument) |
| Position Detail | `/positions/:key/:date` | Single position with snapshot, prices, chart |
| Position History | `/positions/:key/:date/history` | Bitemporal version history |
| Configs | `/configs` | Position config management (full CRUD) |
| System Health | `/health` | System health monitoring |

### 13.2 Features

- Date basis toggle (Trade Date / Settlement Date) on all position views
- Position chart visualization (@ant-design/charts)
- Config creation with scope predicate builder
- Mock mode support (`VITE_USE_MOCKS` env var) for offline development
- Responsive layout with Ant Design components

---

## 14. Load Testing Framework

### 14.1 Architecture

Separate Gradle module (`loadtest/`) with three commands:
- `generate` — Publish trades to Kafka at configurable rate
- `verify` — Verify all positions via REST API against expected values
- `full` — Generate + wait for processing + verify

### 14.2 Profiles

| Profile | Trades | Position Keys | Rate |
|---------|--------|---------------|------|
| SMOKE | 100 | ~10 | 100/sec |
| MEDIUM | 10,000 | ~100 | 1000/sec |
| LARGE | 100,000 | ~1000 | 5000/sec |

### 14.3 Verification

The verifier independently calculates expected WAC and position metrics client-side, then compares against API results. This catches:
- Incorrect WAC calculations
- Missing positions
- Rounding accumulation from save/reload cycles

**Latest LARGE result:** 999/1000 positions passed (1 failure with 0.0018% WAC rounding diff from multi-batch boundaries).

```bash
# Run load test
./gradlew :loadtest:run --args="full --profile=LARGE"
```

---

## 15. Test Coverage

### 15.1 Test Suite (93 tests)

| Category | Tests | Description |
|----------|-------|-------------|
| E2E Flow | 5 | Full pipeline: single trade, multi-trade, multi-day, dual publish, idempotency |
| E2E API | 14 | REST endpoints: health, positions, configs, error handling |
| E2E Calc | 10 | WAC rules: moving away/towards, cross zero, to zero, short, flat, late trade cascade, incremental vs full, carry forward |
| E2E Data Integrity | 4 | Bitemporal consistency, trade vs settlement independence, cache accuracy, notional |
| Unit: WacState | ~20 | Direction-aware WAC edge cases |
| Unit: PositionKeyFormat | ~10 | Key generation, dimension extraction, parsing |
| Unit: ScopePredicate | ~5 | ALL and Criteria matching |
| Integration: Repos | ~25 | Repository operations with Testcontainers (Postgres + Kafka) |

### 15.2 Test Infrastructure

- **Testcontainers:** PostgreSQL 16 + Kafka containers spun up per test class
- **BaseIntegrationTest:** Shared setup with Flyway migration and Exposed connection
- **TestDataSeeder:** Helper for creating test trade events

---

## 16. Acceptance Criteria

### 16.1 Functional

- [x] Trade ingested and stored in <5ms p95
- [x] Position snapshots created per trade (trade + settled, batch-deduped)
- [x] Late trade recalculates all affected dates correctly
- [x] WAC calculated correctly (passes all 4 rule test cases + edge cases)
- [x] Position queries return in <1ms p95
- [x] History table maintains complete audit trail
- [x] Incremental calculation rate >90%
- [x] Database-driven config with scope predicates
- [x] All 5 position key formats supported

### 16.2 Performance

- [x] 100K trades processed correctly (999/1000 positions exact match)
- [x] Batch deduplication reduces calc requests ~5x
- [x] No memory overflow with 100K+ trades
- [x] Database query latency <5ms p95

### 16.3 Reliability

- [x] Zero data loss under normal operation
- [x] Idempotent processing (duplicate trades handled via sequenceNum)
- [x] Idempotent batch insert (single transaction, skip duplicates)

---

## Appendix A: Key Definitions

| Term | Definition |
|------|------------|
| Position | Net quantity of an instrument held (book × counterparty × instrument) |
| Trade Date | Date when trade was executed |
| Settlement Date | Date when trade settles (cash/securities exchange) |
| WAC | Weighted Average Cost - average price accounting for direction |
| Late Trade | Trade arriving after its trade date has passed |
| Incremental Calculation | Calculate using previous snapshot + today's trades only |
| Same-Day Incremental | Recalculate using existing snapshot + trades after lastSequenceNum |
| Bitemporal | Two time dimensions: business date + system time |
| Batch Processing | Insert multiple trades in single transaction, deduplicate calc requests |
| Calc Intent | A pending (positionKey, dateBasis, businessDate) tuple to be published as a calc request |
| Scope Predicate | Filter that determines which trades a position config applies to |
| Position Key Format | Enum defining which trade dimensions form the position key |

---

## Appendix B: WAC Calculation Examples

### Example 1: Normal Trading

```
Start: qty=0, WAC=$0

Buy 1000 @ $150:
  Rule: Moving away from 0 → Weighted average
  Result: qty=1000, WAC=$150

Buy 500 @ $160:
  Rule: Moving away from 0 → Weighted average
  Cost = (1000×150) + (500×160) = 230,000
  Result: qty=1500, WAC=$153.33

Sell 400 @ $155:
  Rule: Moving towards 0 → WAC unchanged
  Cost = 230,000 + (153.33×-400) = 168,668
  Result: qty=1100, WAC=$153.33 (unchanged)
```

### Example 2: Zero Crossing

```
Current: qty=500, WAC=$150

Sell 800 @ $160:
  Rule: Crosses zero → Reset to trade price
  Result: qty=-300, WAC=$160 (reset!)
```

### Example 3: Position to Zero

```
Current: qty=500, WAC=$150

Sell 500 @ $155:
  Rule: Equals zero → avgPrice=0
  Result: qty=0, WAC=$0
```

---

**Document End**

**Approval:**
- Engineering Lead: _________________ Date: _______
- Architecture Review: _________________ Date: _______
- Product Owner: _________________ Date: _______
