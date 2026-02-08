package com.positionservice.e2e

import com.positionservice.db.PositionKeysTable
import com.positionservice.domain.*
import com.positionservice.e2e.TestDataSeeder.bd
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.time.LocalDate

@DisplayName("E2E Data Integrity Tests")
class E2EDataIntegrityTest : BaseIntegrationTest() {

    @Nested
    @DisplayName("Bitemporal Consistency")
    inner class BitemporalConsistency {

        @Test
        @DisplayName("E2E-DATA-001: Save snapshot twice creates 2 history entries with first superseded")
        fun bitemporalConsistency() {
            val positionKey = "BOOK1#GOLDMAN#AAPL"
            val businessDate = LocalDate.of(2025, 1, 20)

            // Insert first trade and calculate
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 100,
                qty = 500,
                price = bd("100.000000"),
                tradeDate = businessDate,
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade1)

            // Verify version 1 exists
            val snapshot1 = snapshotRepo.findSnapshot(positionKey, businessDate, DateBasis.TRADE_DATE)
            assertNotNull(snapshot1)
            assertEquals(1, snapshot1!!.calculationVersion)
            assertEquals(500L, snapshot1.netQuantity)

            // Insert second trade on the same date and recalculate
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 101,
                qty = 300,
                price = bd("110.000000"),
                tradeDate = businessDate,
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade2)

            // Verify snapshot updated to version 2
            val snapshot2 = snapshotRepo.findSnapshot(positionKey, businessDate, DateBasis.TRADE_DATE)
            assertNotNull(snapshot2)
            assertEquals(2, snapshot2!!.calculationVersion)
            assertEquals(800L, snapshot2.netQuantity) // 500 + 300

            // Verify history: 2 entries, first superseded
            val history = snapshotRepo.findSnapshotHistory(positionKey, businessDate, DateBasis.TRADE_DATE)
            assertEquals(2, history.size)

            // First entry (version 1) should be superseded
            assertEquals(1, history[0].calculationVersion)
            assertEquals(500L, history[0].netQuantity)
            assertNotNull(history[0].supersededAt, "First history entry should be superseded")

            // Second entry (version 2) should not be superseded
            assertEquals(2, history[1].calculationVersion)
            assertEquals(800L, history[1].netQuantity)
            assertNull(history[1].supersededAt, "Latest history entry should not be superseded")

            // Second entry should track previous net quantity
            assertEquals(500L, history[1].previousNetQuantity)
        }
    }

    @Nested
    @DisplayName("Trade vs Settlement Date Independence")
    inner class TradeDateVsSettlementDate {

        @Test
        @DisplayName("E2E-DATA-002: Same trade produces different snapshots for trade date vs settlement date")
        fun tradeVsSettlementIndependence() {
            val tradeDate = LocalDate.of(2025, 1, 20)
            val settlementDate = LocalDate.of(2025, 1, 22)

            // Trade 1: trade date Jan 20, settlement Jan 22
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 200,
                qty = 400,
                price = bd("50.000000"),
                tradeDate = tradeDate,
                settlementDate = settlementDate
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade1)

            // Trade 2: trade date also Jan 20, but settlement Jan 23
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 201,
                qty = 600,
                price = bd("55.000000"),
                tradeDate = tradeDate,
                settlementDate = LocalDate.of(2025, 1, 23)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade2)

            val positionKey = "BOOK1#GOLDMAN#AAPL"

            // Trade date basis for Jan 20: both trades contribute
            val tradeDateSnapshot = snapshotRepo.findSnapshot(positionKey, tradeDate, DateBasis.TRADE_DATE)
            assertNotNull(tradeDateSnapshot)
            assertEquals(1000L, tradeDateSnapshot!!.netQuantity) // 400 + 600

            // Settlement date basis for Jan 22: only trade1 settles on this date
            val settlementSnapshot = snapshotRepo.findSnapshot(positionKey, settlementDate, DateBasis.SETTLEMENT_DATE)
            assertNotNull(settlementSnapshot)
            assertEquals(400L, settlementSnapshot!!.netQuantity) // only trade1

            // Settlement date basis for Jan 23: trade2 settles here, incremental from Jan 22
            val settlement23 = snapshotRepo.findSnapshot(positionKey, LocalDate.of(2025, 1, 23), DateBasis.SETTLEMENT_DATE)
            assertNotNull(settlement23)
            assertEquals(1000L, settlement23!!.netQuantity) // 400 (Jan 22 carry) + 600 (trade2)

            // Verify trade date and settlement date snapshots are independent
            assertNotEquals(tradeDateSnapshot.netQuantity, settlementSnapshot.netQuantity)
        }
    }

    @Nested
    @DisplayName("Position Key Cache Accuracy")
    inner class PositionKeyCacheAccuracy {

        @Test
        @DisplayName("E2E-DATA-003: Position key last_trade_date and last_settlement_date reflect MAX values")
        fun positionKeyCacheAccuracy() {
            val positionKey = "BOOK1#GOLDMAN#AAPL"

            // Trade 1: trade date Jan 20, settlement Jan 22
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 300,
                qty = 100,
                price = bd("100.000000"),
                tradeDate = LocalDate.of(2025, 1, 20),
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade1)

            // Trade 2: trade date Jan 25, settlement Jan 27
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 301,
                qty = 200,
                price = bd("105.000000"),
                tradeDate = LocalDate.of(2025, 1, 25),
                settlementDate = LocalDate.of(2025, 1, 27)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade2)

            // Trade 3: trade date Jan 22, settlement Jan 24 (inserted "late" but earlier dates)
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 302,
                qty = 150,
                price = bd("102.000000"),
                tradeDate = LocalDate.of(2025, 1, 22),
                settlementDate = LocalDate.of(2025, 1, 24)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade3)

            // Verify position_keys has MAX of all dates
            val positionKeyRow = transaction {
                PositionKeysTable.selectAll()
                    .where { PositionKeysTable.positionKey eq positionKey }
                    .single()
            }

            val lastTradeDate = positionKeyRow[PositionKeysTable.lastTradeDate]
            val lastSettlementDate = positionKeyRow[PositionKeysTable.lastSettlementDate]

            assertEquals(LocalDate.of(2025, 1, 25), lastTradeDate, "last_trade_date should be the MAX trade date")
            assertEquals(LocalDate.of(2025, 1, 27), lastSettlementDate, "last_settlement_date should be the MAX settlement date")
        }
    }

    @Nested
    @DisplayName("Notional Calculation")
    inner class NotionalCalculation {

        @Test
        @DisplayName("E2E-DATA-004: Total notional equals sum of abs(qty) * price for all trades")
        fun notionalCalculation() {
            val businessDate = LocalDate.of(2025, 1, 20)

            // Trade 1: buy 100 @ 50 => notional = 5000
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 400,
                qty = 100,
                price = bd("50.000000"),
                tradeDate = businessDate,
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade1)

            // Trade 2: sell 30 @ 55 => notional = 1650
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 401,
                qty = -30,
                price = bd("55.000000"),
                tradeDate = businessDate,
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade2)

            // Trade 3: buy 50 @ 48 => notional = 2400
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 402,
                qty = 50,
                price = bd("48.000000"),
                tradeDate = businessDate,
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade3)

            val positionKey = "BOOK1#GOLDMAN#AAPL"
            val snapshot = snapshotRepo.findSnapshot(positionKey, businessDate, DateBasis.TRADE_DATE)
            assertNotNull(snapshot)

            // Expected total notional: 5000 + 1650 + 2400 = 9050
            val expectedNotional = bd("9050.000000")
            assertEquals(
                0,
                expectedNotional.compareTo(snapshot!!.totalNotional),
                "Total notional should be sum(abs(qty)*price). " +
                    "Expected $expectedNotional but got ${snapshot.totalNotional}"
            )

            // Also verify net quantity: 100 - 30 + 50 = 120
            assertEquals(120L, snapshot.netQuantity)
            // Verify gross long: 100 + 50 = 150
            assertEquals(150L, snapshot.grossLong)
            // Verify gross short: abs(-30) = 30
            assertEquals(30L, snapshot.grossShort)
            // Verify trade count: 3
            assertEquals(3, snapshot.tradeCount)

            // Verify WAC price
            val wacPrice = priceRepo.findPrice(positionKey, businessDate, PriceCalculationMethod.WAC, DateBasis.TRADE_DATE)
            assertNotNull(wacPrice)
            // After trades: buy 100@50, sell 30@50(wac), buy 50@48
            // After trade1: qty=100, cost=5000, wac=50
            // After trade2: qty=70, cost=5000+50*(-30)=3500, wac stays 50 (moving towards zero)
            // After trade3: qty=120, cost=3500+48*50=5900, wac=5900/120=49.166667
            val expectedWac = bd("49.166666666667")
            assertEquals(
                0,
                expectedWac.compareTo(wacPrice!!.price),
                "WAC price should be $expectedWac but got ${wacPrice.price}"
            )
        }
    }
}
