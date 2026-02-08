package com.positionservice.e2e

import com.positionservice.db.PositionKeysTable
import com.positionservice.domain.*
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate

@DisplayName("E2E Flow Tests - Full Pipeline")
class E2EFlowTest : BaseIntegrationTest() {

    private fun bd(value: String): BigDecimal = BigDecimal(value).setScale(12, RoundingMode.HALF_UP)

    private val positionKey = "EQUITY-1#GOLDMAN#AAPL"
    private val config = PositionConfig.seed()

    /**
     * Helper to insert a trade and upsert the position key in one step.
     * Does NOT run calculation -- caller decides when to calculate.
     */
    private fun insertTradeAndUpsertKey(trade: TradeEvent): UpsertResult {
        tradeRepo.insertTrade(trade, positionKey)
        return positionKeyRepo.upsertPositionKey(
            positionKey = positionKey,
            configId = config.configId,
            configType = config.type.name,
            configName = config.name,
            book = trade.book,
            counterparty = trade.counterparty,
            instrument = trade.instrument,
            tradeDate = trade.tradeDate,
            settlementDate = trade.settlementDate,
            sequenceNum = trade.sequenceNum
        )
    }

    private fun calcRequest(
        businessDate: LocalDate,
        dateBasis: DateBasis,
        positionId: Long = 1L,
        triggeringSeq: Long = 1L,
        changeReason: ChangeReason = ChangeReason.INITIAL
    ): PositionCalcRequest = TestDataSeeder.createCalcRequest(
        positionKey = positionKey,
        businessDate = businessDate,
        dateBasis = dateBasis,
        positionId = positionId,
        triggeringTradeSequence = triggeringSeq,
        changeReason = changeReason
    )

    @Nested
    @DisplayName("E2E-FLOW-001: Single Trade - Happy Path")
    inner class SingleTradeHappyPath {

        @Test
        @DisplayName("Single buy trade produces correct trade-date and settlement-date snapshots with WAC")
        fun singleTradeHappyPath() {
            val tradeDate = LocalDate.of(2026, 2, 7)
            val settlementDate = LocalDate.of(2026, 2, 9)

            val trade = TestDataSeeder.createTradeEvent(
                seq = 1001,
                book = "EQUITY-1",
                cpty = "GOLDMAN",
                instrument = "AAPL",
                qty = 1000,
                price = bd("150.000000"),
                tradeDate = tradeDate,
                settlementDate = settlementDate
            )

            val upsertResult = insertTradeAndUpsertKey(trade)

            // Calculate for TRADE_DATE
            engine.calculatePosition(calcRequest(
                businessDate = tradeDate,
                dateBasis = DateBasis.TRADE_DATE,
                positionId = upsertResult.positionId,
                triggeringSeq = 1001
            ))

            // Calculate for SETTLEMENT_DATE
            engine.calculatePosition(calcRequest(
                businessDate = settlementDate,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                positionId = upsertResult.positionId,
                triggeringSeq = 1001
            ))

            // Assert trade date snapshot
            val tdSnapshot = snapshotRepo.findSnapshot(positionKey, tradeDate, DateBasis.TRADE_DATE)
            assertNotNull(tdSnapshot, "Trade date snapshot should exist")
            assertEquals(1000L, tdSnapshot!!.netQuantity)
            assertEquals(1000L, tdSnapshot.grossLong)
            assertEquals(0L, tdSnapshot.grossShort)
            assertEquals(1, tdSnapshot.tradeCount)
            assertEquals(CalculationMethod.FULL_RECALC, tdSnapshot.calculationMethod)

            // Assert settlement date snapshot
            val sdSnapshot = snapshotRepo.findSnapshot(positionKey, settlementDate, DateBasis.SETTLEMENT_DATE)
            assertNotNull(sdSnapshot, "Settlement date snapshot should exist")
            assertEquals(1000L, sdSnapshot!!.netQuantity)
            assertEquals(1000L, sdSnapshot.grossLong)
            assertEquals(0L, sdSnapshot.grossShort)
            assertEquals(1, sdSnapshot.tradeCount)
            assertEquals(CalculationMethod.FULL_RECALC, sdSnapshot.calculationMethod)

            // Assert WAC price for trade date
            val tdPrice = priceRepo.findPrice(positionKey, tradeDate, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(tdPrice, "Trade date WAC price should exist")
            assertTrue(
                bd("150.000000").compareTo(tdPrice!!.price) == 0,
                "Trade date WAC should be 150.000000 but was ${tdPrice.price}"
            )

            // Assert WAC price for settlement date
            val sdPrice = priceRepo.findPrice(positionKey, settlementDate, PriceCalculationMethod.WAC, DateBasis.SETTLEMENT_DATE)
            assertNotNull(sdPrice, "Settlement date WAC price should exist")
            assertTrue(
                bd("150.000000").compareTo(sdPrice!!.price) == 0,
                "Settlement date WAC should be 150.000000 but was ${sdPrice.price}"
            )

            // Assert position_keys has correct last_trade_date and last_settlement_date
            val posKey = transaction {
                PositionKeysTable.selectAll()
                    .where { PositionKeysTable.positionKey eq positionKey }
                    .single()
            }
            assertEquals(tradeDate, posKey[PositionKeysTable.lastTradeDate])
            assertEquals(settlementDate, posKey[PositionKeysTable.lastSettlementDate])
        }
    }

    @Nested
    @DisplayName("E2E-FLOW-002: Multiple Trades - Same Position, Same Day")
    inner class MultipleTradesSameDay {

        @Test
        @DisplayName("Three trades same day aggregate correctly with WAC unchanged by sell")
        fun multipleTradesSameDay() {
            val tradeDate = LocalDate.of(2026, 2, 7)
            val settlementDate = LocalDate.of(2026, 2, 9)

            // Buy 1000 @ $150
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 2001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 1000, price = bd("150.000000"),
                tradeDate = tradeDate, settlementDate = settlementDate
            )
            // Buy 500 @ $160
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 2002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("160.000000"),
                tradeDate = tradeDate, settlementDate = settlementDate
            )
            // Sell 400 @ $155
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 2003,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = -400, price = bd("155.000000"),
                tradeDate = tradeDate, settlementDate = settlementDate
            )

            insertTradeAndUpsertKey(trade1)
            insertTradeAndUpsertKey(trade2)
            val upsertResult = insertTradeAndUpsertKey(trade3)

            // Calculate
            engine.calculatePosition(calcRequest(
                businessDate = tradeDate,
                dateBasis = DateBasis.TRADE_DATE,
                positionId = upsertResult.positionId,
                triggeringSeq = 2003
            ))

            val snapshot = snapshotRepo.findSnapshot(positionKey, tradeDate, DateBasis.TRADE_DATE)
            assertNotNull(snapshot, "Snapshot should exist for trade date")
            assertEquals(1100L, snapshot!!.netQuantity, "netQty should be 1000+500-400=1100")
            assertEquals(1500L, snapshot.grossLong, "grossLong should be 1000+500=1500")
            assertEquals(400L, snapshot.grossShort, "grossShort should be 400")
            assertEquals(3, snapshot.tradeCount, "tradeCount should be 3")

            // WAC: Buy 1000@150 -> WAC=150. Buy 500@160 -> WAC=(150000+80000)/1500=153.333333. Sell 400@155 -> moving towards zero, WAC unchanged.
            val wacPrice = priceRepo.findPrice(positionKey, tradeDate, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wacPrice, "WAC price should exist")
            assertTrue(
                bd("153.333333333333").compareTo(wacPrice!!.price) == 0,
                "WAC should be 153.333333 but was ${wacPrice.price}"
            )
        }
    }

    @Nested
    @DisplayName("E2E-FLOW-003: Multi-Day Position Building")
    inner class MultiDayPositionBuilding {

        @Test
        @DisplayName("Three days of trades build position incrementally with correct WAC at each stage")
        fun multiDayPositionBuilding() {
            val day1 = LocalDate.of(2026, 2, 5)
            val day2 = LocalDate.of(2026, 2, 6)
            val day3 = LocalDate.of(2026, 2, 7)

            // Day 1: Buy 1000 @ $150
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 3001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 1000, price = bd("150.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)

            // Day 2: Buy 500 @ $160
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 3002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("160.000000"),
                tradeDate = day2, settlementDate = day2.plusDays(2)
            )
            insertTradeAndUpsertKey(trade2)

            // Day 3: Sell 300 @ $155
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 3003,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = -300, price = bd("155.000000"),
                tradeDate = day3, settlementDate = day3.plusDays(2)
            )
            insertTradeAndUpsertKey(trade3)

            // Calculate Day 1 (FULL - no previous exists)
            engine.calculatePosition(calcRequest(businessDate = day1, dateBasis = DateBasis.TRADE_DATE, triggeringSeq = 3001))

            // Calculate Day 2 (INCREMENTAL - Day 1 exists)
            engine.calculatePosition(calcRequest(businessDate = day2, dateBasis = DateBasis.TRADE_DATE, triggeringSeq = 3002))

            // Calculate Day 3 (INCREMENTAL - Day 2 exists)
            engine.calculatePosition(calcRequest(businessDate = day3, dateBasis = DateBasis.TRADE_DATE, triggeringSeq = 3003))

            // Assert Day 1
            val snap1 = snapshotRepo.findSnapshot(positionKey, day1, DateBasis.TRADE_DATE)
            assertNotNull(snap1, "Day 1 snapshot should exist")
            assertEquals(1000L, snap1!!.netQuantity)
            assertEquals(CalculationMethod.FULL_RECALC, snap1.calculationMethod)
            val wac1 = priceRepo.findPrice(positionKey, day1, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac1)
            assertTrue(
                bd("150.000000").compareTo(wac1!!.price) == 0,
                "Day 1 WAC should be 150.000000 but was ${wac1.price}"
            )

            // Assert Day 2
            val snap2 = snapshotRepo.findSnapshot(positionKey, day2, DateBasis.TRADE_DATE)
            assertNotNull(snap2, "Day 2 snapshot should exist")
            assertEquals(1500L, snap2!!.netQuantity, "Day 2 netQty should be 1000+500=1500")
            assertEquals(CalculationMethod.INCREMENTAL, snap2.calculationMethod)
            val wac2 = priceRepo.findPrice(positionKey, day2, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac2)
            assertTrue(
                bd("153.333333333333").compareTo(wac2!!.price) == 0,
                "Day 2 WAC should be 153.333333 but was ${wac2.price}"
            )

            // Assert Day 3
            val snap3 = snapshotRepo.findSnapshot(positionKey, day3, DateBasis.TRADE_DATE)
            assertNotNull(snap3, "Day 3 snapshot should exist")
            assertEquals(1200L, snap3!!.netQuantity, "Day 3 netQty should be 1500-300=1200")
            assertEquals(CalculationMethod.INCREMENTAL, snap3.calculationMethod)
            val wac3 = priceRepo.findPrice(positionKey, day3, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac3)
            assertTrue(
                bd("153.333333333333").compareTo(wac3!!.price) == 0,
                "Day 3 WAC should be 153.333333 (unchanged by sell) but was ${wac3.price}"
            )
        }
    }

    @Nested
    @DisplayName("E2E-FLOW-004: Dual Publishing - Trade + Settlement")
    inner class DualPublishing {

        @Test
        @DisplayName("Single trade produces snapshots in both trade-date and settlement-date tables with identical metrics")
        fun dualPublishing() {
            val tradeDate = LocalDate.of(2026, 2, 5)
            val settlementDate = LocalDate.of(2026, 2, 7)

            val trade = TestDataSeeder.createTradeEvent(
                seq = 4001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("200.000000"),
                tradeDate = tradeDate, settlementDate = settlementDate
            )
            val upsertResult = insertTradeAndUpsertKey(trade)

            // Calculate TRADE_DATE
            engine.calculatePosition(calcRequest(
                businessDate = tradeDate,
                dateBasis = DateBasis.TRADE_DATE,
                positionId = upsertResult.positionId,
                triggeringSeq = 4001
            ))

            // Calculate SETTLEMENT_DATE
            engine.calculatePosition(calcRequest(
                businessDate = settlementDate,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                positionId = upsertResult.positionId,
                triggeringSeq = 4001
            ))

            // Assert trade date snapshot exists on 2026-02-05
            val tdSnap = snapshotRepo.findSnapshot(positionKey, tradeDate, DateBasis.TRADE_DATE)
            assertNotNull(tdSnap, "Trade date snapshot should exist on 2026-02-05")

            // Assert settlement date snapshot exists on 2026-02-07
            val sdSnap = snapshotRepo.findSnapshot(positionKey, settlementDate, DateBasis.SETTLEMENT_DATE)
            assertNotNull(sdSnap, "Settlement date snapshot should exist on 2026-02-07")

            // Both should have identical metrics
            assertEquals(tdSnap!!.netQuantity, sdSnap!!.netQuantity, "netQuantity should match")
            assertEquals(tdSnap.grossLong, sdSnap.grossLong, "grossLong should match")
            assertEquals(tdSnap.grossShort, sdSnap.grossShort, "grossShort should match")
            assertEquals(tdSnap.tradeCount, sdSnap.tradeCount, "tradeCount should match")
            assertTrue(
                tdSnap.totalNotional.compareTo(sdSnap.totalNotional) == 0,
                "totalNotional should match"
            )

            // Verify specific values
            assertEquals(500L, tdSnap.netQuantity)
            assertEquals(500L, tdSnap.grossLong)
            assertEquals(0L, tdSnap.grossShort)
            assertEquals(1, tdSnap.tradeCount)
        }
    }

    @Nested
    @DisplayName("E2E-FLOW-005: Idempotent Trade Processing")
    inner class IdempotentTradeProcessing {

        @Test
        @DisplayName("Inserting the same trade twice returns false and does not duplicate data")
        fun idempotentTradeProcessing() {
            val tradeDate = LocalDate.of(2026, 2, 7)
            val settlementDate = LocalDate.of(2026, 2, 9)

            val trade = TestDataSeeder.createTradeEvent(
                seq = 5001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 100, price = bd("300.000000"),
                tradeDate = tradeDate, settlementDate = settlementDate
            )

            // First insert - should succeed
            val firstInsert = tradeRepo.insertTrade(trade, positionKey)
            assertTrue(firstInsert, "First insert should return true")

            // Second insert with same sequenceNum - should be idempotent
            val secondInsert = tradeRepo.insertTrade(trade, positionKey)
            assertFalse(secondInsert, "Second insert should return false (idempotent)")

            // Upsert position key and calculate
            val upsertResult = insertTradeAndUpsertKey(trade)
            engine.calculatePosition(calcRequest(
                businessDate = tradeDate,
                dateBasis = DateBasis.TRADE_DATE,
                positionId = upsertResult.positionId,
                triggeringSeq = 5001
            ))

            // Assert only 1 trade affected position
            val snapshot = snapshotRepo.findSnapshot(positionKey, tradeDate, DateBasis.TRADE_DATE)
            assertNotNull(snapshot, "Snapshot should exist")
            assertEquals(100L, snapshot!!.netQuantity, "Only 1 trade should be counted, netQty=100")
            assertEquals(1, snapshot.tradeCount, "Only 1 trade should be counted")
        }
    }
}
