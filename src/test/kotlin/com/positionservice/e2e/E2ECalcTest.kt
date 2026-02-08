package com.positionservice.e2e

import com.positionservice.domain.*
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate

@DisplayName("E2E Calculation Tests - WAC Correctness Through Real DB")
class E2ECalcTest : BaseIntegrationTest() {

    private fun bd(value: String): BigDecimal = BigDecimal(value).setScale(12, RoundingMode.HALF_UP)

    private val positionKey = "EQUITY-1#GOLDMAN#AAPL"
    private val config = PositionConfig.seed()

    /**
     * Helper to insert a trade and upsert the position key.
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
        dateBasis: DateBasis = DateBasis.TRADE_DATE,
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
    @DisplayName("E2E-CALC-001: WAC Rule - Moving Away from Zero (Buy into Long)")
    inner class WacMovingAwayFromZero {

        @Test
        @DisplayName("Two buys from flat produce correct weighted average cost")
        fun movingAwayFromZeroBuyIntoLong() {
            val day1 = LocalDate.of(2026, 2, 7)

            // Buy 1000 @ $150
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 1001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 1000, price = bd("150.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            // Buy 500 @ $160
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 1002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("160.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )

            insertTradeAndUpsertKey(trade1)
            insertTradeAndUpsertKey(trade2)

            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 1002))

            val wac = priceRepo.findPrice(positionKey, day1, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac, "WAC price should exist")
            // WAC = (1000*150 + 500*160) / 1500 = (150000+80000)/1500 = 230000/1500 = 153.333333
            assertTrue(
                bd("153.333333333333").compareTo(wac!!.price) == 0,
                "WAC should be (150000+80000)/1500 = 153.333333 but was ${wac.price}"
            )

            val snapshot = snapshotRepo.findSnapshot(positionKey, day1, DateBasis.TRADE_DATE)
            assertNotNull(snapshot)
            assertEquals(1500L, snapshot!!.netQuantity)
        }
    }

    @Nested
    @DisplayName("E2E-CALC-002: WAC Rule - Moving Towards Zero (Sell from Long)")
    inner class WacMovingTowardsZero {

        @Test
        @DisplayName("Selling from long position does not change WAC")
        fun movingTowardsZeroSellFromLong() {
            val day1 = LocalDate.of(2026, 2, 6)
            val day2 = LocalDate.of(2026, 2, 7)

            // Day 1: Buy 1000 @ $150, Buy 500 @ $160
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 2001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 1000, price = bd("150.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 2002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("160.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)
            insertTradeAndUpsertKey(trade2)

            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 2002))

            // Verify Day 1 result: qty=1500, WAC=153.333333
            val wac1 = priceRepo.findPrice(positionKey, day1, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac1)
            assertTrue(
                bd("153.333333333333").compareTo(wac1!!.price) == 0,
                "Day 1 WAC should be 153.333333 but was ${wac1.price}"
            )

            // Day 2: Sell 400 @ $170 (moving towards zero)
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 2003,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = -400, price = bd("170.000000"),
                tradeDate = day2, settlementDate = day2.plusDays(2)
            )
            insertTradeAndUpsertKey(trade3)

            engine.calculatePosition(calcRequest(businessDate = day2, triggeringSeq = 2003))

            val snap2 = snapshotRepo.findSnapshot(positionKey, day2, DateBasis.TRADE_DATE)
            assertNotNull(snap2)
            assertEquals(1100L, snap2!!.netQuantity, "netQty should be 1500-400=1100")

            // WAC should be UNCHANGED at 153.333333
            val wac2 = priceRepo.findPrice(positionKey, day2, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac2)
            assertTrue(
                bd("153.333333333333").compareTo(wac2!!.price) == 0,
                "WAC should remain 153.333333 (unchanged by sell towards zero) but was ${wac2.price}"
            )
        }
    }

    @Nested
    @DisplayName("E2E-CALC-003: WAC Rule - Cross Zero (Long to Short)")
    inner class WacCrossZero {

        @Test
        @DisplayName("Selling more than held crosses zero and resets WAC to trade price")
        fun crossZeroLongToShort() {
            val day1 = LocalDate.of(2026, 2, 6)
            val day2 = LocalDate.of(2026, 2, 7)

            // Day 1: Buy 500 @ $150
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 3001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("150.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)
            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 3001))

            // Verify Day 1
            val snap1 = snapshotRepo.findSnapshot(positionKey, day1, DateBasis.TRADE_DATE)
            assertNotNull(snap1)
            assertEquals(500L, snap1!!.netQuantity)

            // Day 2: Sell 800 @ $160 (crosses zero: 500 -> -300)
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 3002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = -800, price = bd("160.000000"),
                tradeDate = day2, settlementDate = day2.plusDays(2)
            )
            insertTradeAndUpsertKey(trade2)
            engine.calculatePosition(calcRequest(businessDate = day2, triggeringSeq = 3002))

            val snap2 = snapshotRepo.findSnapshot(positionKey, day2, DateBasis.TRADE_DATE)
            assertNotNull(snap2)
            assertEquals(-300L, snap2!!.netQuantity, "netQty should be 500-800=-300")

            // WAC should RESET to $160 (the trade price that crossed zero)
            val wac2 = priceRepo.findPrice(positionKey, day2, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac2)
            assertTrue(
                bd("160.000000").compareTo(wac2!!.price) == 0,
                "WAC should reset to 160.000000 on zero-cross but was ${wac2.price}"
            )
        }
    }

    @Nested
    @DisplayName("E2E-CALC-004: WAC Rule - Position to Zero")
    inner class WacPositionToZero {

        @Test
        @DisplayName("Closing position entirely sets WAC to zero")
        fun positionToZero() {
            val day1 = LocalDate.of(2026, 2, 6)
            val day2 = LocalDate.of(2026, 2, 7)

            // Day 1: Buy 500 @ $150
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 4001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("150.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)
            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 4001))

            // Day 2: Sell 500 @ $155 (position goes to exactly zero)
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 4002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = -500, price = bd("155.000000"),
                tradeDate = day2, settlementDate = day2.plusDays(2)
            )
            insertTradeAndUpsertKey(trade2)
            engine.calculatePosition(calcRequest(businessDate = day2, triggeringSeq = 4002))

            val snap2 = snapshotRepo.findSnapshot(positionKey, day2, DateBasis.TRADE_DATE)
            assertNotNull(snap2)
            assertEquals(0L, snap2!!.netQuantity, "netQty should be 0")

            // WAC should be $0 when position is zero
            val wac2 = priceRepo.findPrice(positionKey, day2, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac2)
            assertTrue(
                bd("0.000000").compareTo(wac2!!.price) == 0,
                "WAC should be 0 when position is flat but was ${wac2.price}"
            )
        }
    }

    @Nested
    @DisplayName("E2E-CALC-005: WAC - Short Position Building")
    inner class WacShortPositionBuilding {

        @Test
        @DisplayName("Short selling from flat builds short position with correct WAC")
        fun shortPositionBuilding() {
            val day1 = LocalDate.of(2026, 2, 7)

            // Sell 1000 @ $100 (go short)
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 5001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = -1000, price = bd("100.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            // Sell 500 @ $110 (add to short)
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 5002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = -500, price = bd("110.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )

            insertTradeAndUpsertKey(trade1)
            insertTradeAndUpsertKey(trade2)

            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 5002))

            val snapshot = snapshotRepo.findSnapshot(positionKey, day1, DateBasis.TRADE_DATE)
            assertNotNull(snapshot)
            assertEquals(-1500L, snapshot!!.netQuantity, "netQty should be -1500")

            // WAC for short: totalCostBasis = -1000*100 + (-500)*110 = -100000 + -55000 = -155000
            // avgPrice = abs(-155000) / abs(-1500) = 155000/1500 = 103.333333
            val wac = priceRepo.findPrice(positionKey, day1, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac)
            assertTrue(
                bd("103.333333333333").compareTo(wac!!.price) == 0,
                "Short WAC should be 103.333333 but was ${wac.price}"
            )
        }
    }

    @Nested
    @DisplayName("E2E-CALC-006: WAC - First Trade from Flat")
    inner class WacFirstTradeFromFlat {

        @Test
        @DisplayName("First trade from flat position uses trade price directly as WAC")
        fun firstTradeFromFlat() {
            val day1 = LocalDate.of(2026, 2, 7)

            // Single buy from flat
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 6001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 1000, price = bd("250.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)
            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 6001))

            val wac = priceRepo.findPrice(positionKey, day1, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac, "WAC should exist")
            assertTrue(
                bd("250.000000").compareTo(wac!!.price) == 0,
                "First trade from flat should set WAC to trade price 250.000000 but was ${wac.price}"
            )
        }
    }

    @Nested
    @DisplayName("E2E-CALC-007: Late Trade - Cascade Recalculation")
    inner class LateTradeCascadeRecalculation {

        @Test
        @DisplayName("Late trade triggers cascade recalculation across subsequent dates")
        fun lateTradeCascadeRecalculation() {
            val jan20 = LocalDate.of(2026, 1, 20)
            val jan21 = LocalDate.of(2026, 1, 21)
            val jan22 = LocalDate.of(2026, 1, 22)
            val jan25 = LocalDate.of(2026, 1, 25)

            // Build initial positions: Jan 20 Buy 100@$50
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 7001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 100, price = bd("50.000000"),
                tradeDate = jan20, settlementDate = jan20.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)

            // Jan 22 Buy 200@$55
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 7002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 200, price = bd("55.000000"),
                tradeDate = jan22, settlementDate = jan22.plusDays(2)
            )
            insertTradeAndUpsertKey(trade2)

            // Jan 25 Buy 150@$52
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 7003,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 150, price = bd("52.000000"),
                tradeDate = jan25, settlementDate = jan25.plusDays(2)
            )
            insertTradeAndUpsertKey(trade3)

            // Calculate initial positions in order
            engine.calculatePosition(calcRequest(businessDate = jan20, triggeringSeq = 7001))
            engine.calculatePosition(calcRequest(businessDate = jan22, triggeringSeq = 7002))
            engine.calculatePosition(calcRequest(businessDate = jan25, triggeringSeq = 7003))

            // Record initial Jan 20 snapshot for later comparison
            val jan20SnapBefore = snapshotRepo.findSnapshot(positionKey, jan20, DateBasis.TRADE_DATE)
            assertNotNull(jan20SnapBefore)
            assertEquals(100L, jan20SnapBefore!!.netQuantity)

            // Record initial Jan 25 snapshot
            val jan25SnapBefore = snapshotRepo.findSnapshot(positionKey, jan25, DateBasis.TRADE_DATE)
            assertNotNull(jan25SnapBefore)
            val jan25VersionBefore = jan25SnapBefore!!.calculationVersion

            // Record initial Jan 22 snapshot
            val jan22SnapBefore = snapshotRepo.findSnapshot(positionKey, jan22, DateBasis.TRADE_DATE)
            assertNotNull(jan22SnapBefore)
            val jan22VersionBefore = jan22SnapBefore!!.calculationVersion

            // Now insert a LATE trade for Jan 21: Buy 300@$48
            val lateTrade = TestDataSeeder.createTradeEvent(
                seq = 7004,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 300, price = bd("48.000000"),
                tradeDate = jan21, settlementDate = jan21.plusDays(2)
            )
            insertTradeAndUpsertKey(lateTrade)

            // Cascade recalculation:
            // 1. Calculate Jan 21 (new snapshot, FULL since no Jan 20 snapshot exists for day before = Jan 20 exists!)
            engine.calculatePosition(calcRequest(
                businessDate = jan21,
                triggeringSeq = 7004,
                changeReason = ChangeReason.LATE_TRADE
            ))

            // 2. Recalculate Jan 22 (INCREMENTAL from Jan 21)
            engine.calculatePosition(calcRequest(
                businessDate = jan22,
                triggeringSeq = 7004,
                changeReason = ChangeReason.LATE_TRADE
            ))

            // 3. Recalculate Jan 25 (INCREMENTAL from Jan 22 -- note: no Jan 23/24 snapshots,
            //    so this will be FULL_RECALC since no Jan 24 snapshot exists)
            engine.calculatePosition(calcRequest(
                businessDate = jan25,
                triggeringSeq = 7004,
                changeReason = ChangeReason.LATE_TRADE
            ))

            // Assert: Jan 20 UNCHANGED
            val jan20SnapAfter = snapshotRepo.findSnapshot(positionKey, jan20, DateBasis.TRADE_DATE)
            assertNotNull(jan20SnapAfter)
            assertEquals(100L, jan20SnapAfter!!.netQuantity, "Jan 20 should be unchanged at 100")
            assertEquals(jan20SnapBefore.calculationVersion, jan20SnapAfter.calculationVersion,
                "Jan 20 version should be unchanged")

            // Assert: Jan 21 new snapshot exists
            // Engine looks for Jan 20 snapshot (businessDate-1) -> found (100 qty)
            // So Jan 21 is INCREMENTAL: 100 (previous) + 300 (today) = 400
            val jan21Snap = snapshotRepo.findSnapshot(positionKey, jan21, DateBasis.TRADE_DATE)
            assertNotNull(jan21Snap, "Jan 21 snapshot should exist after late trade")
            assertEquals(400L, jan21Snap!!.netQuantity, "Jan 21 netQty should be 100 (Jan 20) + 300 (late trade) = 400")

            // Assert: Jan 22 recalculated with higher version
            // Engine looks for Jan 21 snapshot (businessDate-1) -> found (400 qty)
            // So Jan 22 is INCREMENTAL: 400 (Jan 21) + 200 (today's trades) = 600
            val jan22SnapAfter = snapshotRepo.findSnapshot(positionKey, jan22, DateBasis.TRADE_DATE)
            assertNotNull(jan22SnapAfter)
            assertTrue(jan22SnapAfter!!.calculationVersion > jan22VersionBefore,
                "Jan 22 version should increase after recalculation. Before: $jan22VersionBefore, After: ${jan22SnapAfter.calculationVersion}")
            assertEquals(600L, jan22SnapAfter.netQuantity, "Jan 22 netQty should be 400 (Jan 21) + 200 (today) = 600")

            // Assert: Jan 25 recalculated
            // Engine looks for Jan 24 snapshot (businessDate-1) -> NOT found
            // So Jan 25 is FULL_RECALC: only Jan 25 trades (150@$52) = netQty 150
            // Same quantity as before but version should increase since saveSnapshot overwrites
            val jan25SnapAfter = snapshotRepo.findSnapshot(positionKey, jan25, DateBasis.TRADE_DATE)
            assertNotNull(jan25SnapAfter)
            assertEquals(150L, jan25SnapAfter!!.netQuantity, "Jan 25 netQty should be 150 (FULL_RECALC of Jan 25 trades only)")
            assertTrue(jan25SnapAfter.calculationVersion > jan25VersionBefore,
                "Jan 25 version should increase after recalculation. Before: $jan25VersionBefore, After: ${jan25SnapAfter.calculationVersion}")

            // Assert history entries have change_reason = LATE_TRADE for Jan 22
            val jan22History = snapshotRepo.findSnapshotHistory(positionKey, jan22, DateBasis.TRADE_DATE)
            assertTrue(jan22History.size >= 2,
                "Jan 22 should have at least 2 history entries (original + recalc)")
            val latestJan22History = jan22History.last()
            assertEquals("LATE_TRADE", latestJan22History.changeReason,
                "Latest Jan 22 history entry should have LATE_TRADE change reason")

            // Assert history entries have change_reason = LATE_TRADE for Jan 25
            val jan25History = snapshotRepo.findSnapshotHistory(positionKey, jan25, DateBasis.TRADE_DATE)
            assertTrue(jan25History.size >= 2,
                "Jan 25 should have at least 2 history entries (original + recalc)")
            val latestJan25History = jan25History.last()
            assertEquals("LATE_TRADE", latestJan25History.changeReason,
                "Latest Jan 25 history entry should have LATE_TRADE change reason")
        }
    }

    @Nested
    @DisplayName("E2E-CALC-008: Late Trade - Settlement Cascade")
    inner class LateTradeSettlementCascade {

        @Test
        @DisplayName("Late trade triggers cascade recalculation on settlement date basis")
        fun lateTradeSettlementCascade() {
            val jan22 = LocalDate.of(2026, 1, 22)
            val jan23 = LocalDate.of(2026, 1, 23)
            val jan24 = LocalDate.of(2026, 1, 24)
            val jan27 = LocalDate.of(2026, 1, 27)

            // Trade 1: tradeDate=Jan 20, settlementDate=Jan 22 -> Buy 100@$50
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 8001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 100, price = bd("50.000000"),
                tradeDate = LocalDate.of(2026, 1, 20),
                settlementDate = jan22
            )
            insertTradeAndUpsertKey(trade1)

            // Trade 2: tradeDate=Jan 22, settlementDate=Jan 24 -> Buy 200@$55
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 8002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 200, price = bd("55.000000"),
                tradeDate = LocalDate.of(2026, 1, 22),
                settlementDate = jan24
            )
            insertTradeAndUpsertKey(trade2)

            // Trade 3: tradeDate=Jan 23, settlementDate=Jan 27 -> Buy 150@$52
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 8003,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 150, price = bd("52.000000"),
                tradeDate = LocalDate.of(2026, 1, 23),
                settlementDate = jan27
            )
            insertTradeAndUpsertKey(trade3)

            // Calculate initial settled positions
            engine.calculatePosition(calcRequest(
                businessDate = jan22,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                triggeringSeq = 8001
            ))
            engine.calculatePosition(calcRequest(
                businessDate = jan24,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                triggeringSeq = 8002
            ))
            engine.calculatePosition(calcRequest(
                businessDate = jan27,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                triggeringSeq = 8003
            ))

            // Verify initial settled positions
            val jan22SnapBefore = snapshotRepo.findSnapshot(positionKey, jan22, DateBasis.SETTLEMENT_DATE)
            assertNotNull(jan22SnapBefore)
            assertEquals(100L, jan22SnapBefore!!.netQuantity)
            val jan24VersionBefore = snapshotRepo.findSnapshot(positionKey, jan24, DateBasis.SETTLEMENT_DATE)!!.calculationVersion

            // Insert late trade: tradeDate=Jan 21, settlementDate=Jan 23 -> Buy 300@$48
            val lateTrade = TestDataSeeder.createTradeEvent(
                seq = 8004,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 300, price = bd("48.000000"),
                tradeDate = LocalDate.of(2026, 1, 21),
                settlementDate = jan23
            )
            insertTradeAndUpsertKey(lateTrade)

            // Cascade recalculation on settlement basis
            // 1. Calculate Jan 23 (new -- late trade settles here)
            engine.calculatePosition(calcRequest(
                businessDate = jan23,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                triggeringSeq = 8004,
                changeReason = ChangeReason.LATE_TRADE
            ))

            // 2. Recalculate Jan 24 (incremental from Jan 23)
            engine.calculatePosition(calcRequest(
                businessDate = jan24,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                triggeringSeq = 8004,
                changeReason = ChangeReason.LATE_TRADE
            ))

            // 3. Recalculate Jan 27
            engine.calculatePosition(calcRequest(
                businessDate = jan27,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                triggeringSeq = 8004,
                changeReason = ChangeReason.LATE_TRADE
            ))

            // Assert: Jan 22 UNCHANGED
            val jan22SnapAfter = snapshotRepo.findSnapshot(positionKey, jan22, DateBasis.SETTLEMENT_DATE)
            assertNotNull(jan22SnapAfter)
            assertEquals(100L, jan22SnapAfter!!.netQuantity, "Jan 22 settled should be unchanged")

            // Assert: Jan 23 new snapshot exists
            // Engine looks for Jan 22 settled snapshot (businessDate-1) -> found (100 qty)
            // So Jan 23 is INCREMENTAL: 100 (Jan 22) + 300 (today's late trade) = 400
            val jan23Snap = snapshotRepo.findSnapshot(positionKey, jan23, DateBasis.SETTLEMENT_DATE)
            assertNotNull(jan23Snap, "Jan 23 settled snapshot should exist after late trade")
            assertEquals(400L, jan23Snap!!.netQuantity, "Jan 23 settled netQty should be 100 (Jan 22) + 300 (late trade) = 400")

            // Assert: Jan 24 recalculated with higher version
            // Engine looks for Jan 23 settled snapshot (businessDate-1) -> found (400 qty)
            // So Jan 24 is INCREMENTAL: 400 (Jan 23) + 200 (today's trades) = 600
            val jan24SnapAfter = snapshotRepo.findSnapshot(positionKey, jan24, DateBasis.SETTLEMENT_DATE)
            assertNotNull(jan24SnapAfter)
            assertTrue(jan24SnapAfter!!.calculationVersion > jan24VersionBefore,
                "Jan 24 settled version should increase after recalculation")
            assertEquals(600L, jan24SnapAfter.netQuantity, "Jan 24 settled netQty should be 400 (Jan 23) + 200 (today) = 600")

            // Assert history for Jan 24 has LATE_TRADE
            val jan24History = snapshotRepo.findSnapshotHistory(positionKey, jan24, DateBasis.SETTLEMENT_DATE)
            assertTrue(jan24History.size >= 2,
                "Jan 24 settled should have at least 2 history entries")
            assertEquals("LATE_TRADE", jan24History.last().changeReason,
                "Latest Jan 24 settled history should have LATE_TRADE change reason")
        }
    }

    @Nested
    @DisplayName("E2E-CALC-009: Incremental vs Full Calculation Method")
    inner class IncrementalVsFullMethod {

        @Test
        @DisplayName("Day 1 uses FULL_RECALC, Day 2 uses INCREMENTAL when previous day exists")
        fun incrementalVsFullMethod() {
            val day1 = LocalDate.of(2026, 2, 6)
            val day2 = LocalDate.of(2026, 2, 7)

            // Day 1 trade
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 9001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 1000, price = bd("100.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)

            // Day 2 trade
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 9002,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 500, price = bd("105.000000"),
                tradeDate = day2, settlementDate = day2.plusDays(2)
            )
            insertTradeAndUpsertKey(trade2)

            // Calculate Day 1 - no previous exists
            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 9001))

            val snap1 = snapshotRepo.findSnapshot(positionKey, day1, DateBasis.TRADE_DATE)
            assertNotNull(snap1)
            assertEquals(CalculationMethod.FULL_RECALC, snap1!!.calculationMethod,
                "Day 1 should use FULL_RECALC since no previous day snapshot exists")

            // Calculate Day 2 - Day 1 exists as previous
            engine.calculatePosition(calcRequest(businessDate = day2, triggeringSeq = 9002))

            val snap2 = snapshotRepo.findSnapshot(positionKey, day2, DateBasis.TRADE_DATE)
            assertNotNull(snap2)
            assertEquals(CalculationMethod.INCREMENTAL, snap2!!.calculationMethod,
                "Day 2 should use INCREMENTAL since Day 1 snapshot exists")
        }
    }

    @Nested
    @DisplayName("E2E-CALC-010: No Trades Day - Carry Forward")
    inner class NoTradesDayCarryForward {

        @Test
        @DisplayName("Day with no trades carries forward previous day snapshot values")
        fun noTradesDayCarryForward() {
            val day1 = LocalDate.of(2026, 2, 6)
            val day2 = LocalDate.of(2026, 2, 7)

            // Day 1: Buy 1000 @ $150
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 10001,
                book = "EQUITY-1", cpty = "GOLDMAN", instrument = "AAPL",
                qty = 1000, price = bd("150.000000"),
                tradeDate = day1, settlementDate = day1.plusDays(2)
            )
            insertTradeAndUpsertKey(trade1)
            engine.calculatePosition(calcRequest(businessDate = day1, triggeringSeq = 10001))

            // Verify Day 1
            val snap1 = snapshotRepo.findSnapshot(positionKey, day1, DateBasis.TRADE_DATE)
            assertNotNull(snap1)
            assertEquals(1000L, snap1!!.netQuantity)
            val wac1 = priceRepo.findPrice(positionKey, day1, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac1)
            assertTrue(
                bd("150.000000").compareTo(wac1!!.price) == 0,
                "Day 1 WAC should be 150.000000"
            )

            // Day 2: NO trades, but engine.calculatePosition is called
            // (incremental path: previous day exists, no trades today -> carry forward)
            engine.calculatePosition(calcRequest(businessDate = day2, triggeringSeq = 10001))

            // Assert Day 2 snapshot carries forward Day 1 values
            val snap2 = snapshotRepo.findSnapshot(positionKey, day2, DateBasis.TRADE_DATE)
            assertNotNull(snap2, "Day 2 snapshot should exist (carried forward)")
            assertEquals(snap1.netQuantity, snap2!!.netQuantity,
                "Day 2 netQuantity should equal Day 1")
            assertEquals(snap1.grossLong, snap2.grossLong,
                "Day 2 grossLong should equal Day 1")
            assertEquals(snap1.grossShort, snap2.grossShort,
                "Day 2 grossShort should equal Day 1")
            assertEquals(snap1.tradeCount, snap2.tradeCount,
                "Day 2 tradeCount should equal Day 1")
            assertTrue(
                snap1.totalNotional.compareTo(snap2.totalNotional) == 0,
                "Day 2 totalNotional should equal Day 1"
            )
            assertEquals(CalculationMethod.INCREMENTAL, snap2.calculationMethod,
                "Day 2 should use INCREMENTAL method (carry forward)")

            // Assert WAC carried forward
            val wac2 = priceRepo.findPrice(positionKey, day2, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wac2, "Day 2 WAC should exist (carried forward)")
            assertTrue(
                bd("150.000000").compareTo(wac2!!.price) == 0,
                "Day 2 WAC should be carried forward as 150.000000 but was ${wac2.price}"
            )
        }
    }
}
