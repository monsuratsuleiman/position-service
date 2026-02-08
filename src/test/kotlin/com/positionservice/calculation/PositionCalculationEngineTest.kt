package com.positionservice.calculation

import com.positionservice.db.PositionAveragePriceRepository
import com.positionservice.db.PositionSnapshotRepository
import com.positionservice.db.PositionTradeRepository
import com.positionservice.db.TradeRecord
import com.positionservice.domain.*
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.time.LocalDate

class PositionCalculationEngineTest {

    private lateinit var tradeRepo: FakeTradeRepo
    private lateinit var snapshotRepo: FakeSnapshotRepo
    private lateinit var priceRepo: FakePriceRepo
    private lateinit var engine: PositionCalculationEngine

    private fun bd(value: String): BigDecimal = BigDecimal(value).setScale(12, RoundingMode.HALF_UP)

    @BeforeEach
    fun setup() {
        tradeRepo = FakeTradeRepo()
        snapshotRepo = FakeSnapshotRepo()
        priceRepo = FakePriceRepo()
        engine = PositionCalculationEngine(tradeRepo, snapshotRepo, priceRepo)
    }

    @Test
    fun `full calculation creates snapshot from trades`() {
        tradeRepo.metrics["BOOK1#GOLDMAN#AAPL|2025-01-20|TRADE_DATE"] = TradeMetrics(
            netQuantity = 1000,
            grossLong = 1000,
            grossShort = 0,
            tradeCount = 1,
            totalNotional = bd("150000"),
            lastSequenceNum = 1,
            lastTradeTime = Instant.now()
        )
        tradeRepo.trades["BOOK1#GOLDMAN#AAPL|2025-01-20|TRADE_DATE"] = listOf(
            TradeRecord(1, 1000, bd("150"))
        )

        val request = PositionCalcRequest(
            requestId = "test-1",
            positionId = 1,
            positionKey = "BOOK1#GOLDMAN#AAPL",
            dateBasis = DateBasis.TRADE_DATE,
            businessDate = LocalDate.of(2025, 1, 20),
            priceMethods = setOf(PriceCalculationMethod.WAC),
            triggeringTradeSequence = 1
        )

        engine.calculatePosition(request)

        val saved = snapshotRepo.savedSnapshots.last()
        assertEquals(1000L, saved.netQuantity)
        assertEquals(1000L, saved.grossLong)
        assertEquals(0L, saved.grossShort)
        assertEquals(1, saved.tradeCount)
        assertEquals(CalculationMethod.FULL_RECALC, saved.calculationMethod)

        val savedPrice = priceRepo.savedPrices.last()
        assertEquals(bd("150"), savedPrice.price)
    }

    @Test
    fun `incremental calculation uses previous snapshot`() {
        // Previous day snapshot
        val previousDate = LocalDate.of(2025, 1, 19)
        snapshotRepo.snapshots["BOOK1#GOLDMAN#AAPL|2025-01-19|TRADE_DATE"] = PositionSnapshot(
            positionKey = "BOOK1#GOLDMAN#AAPL",
            businessDate = previousDate,
            netQuantity = 1000,
            grossLong = 1000,
            grossShort = 0,
            tradeCount = 1,
            totalNotional = bd("150000"),
            lastSequenceNum = 1
        )
        priceRepo.prices["BOOK1#GOLDMAN#AAPL|2025-01-19|WAC|TRADE_DATE"] = PositionAveragePrice(
            positionKey = "BOOK1#GOLDMAN#AAPL",
            businessDate = previousDate,
            priceMethod = PriceCalculationMethod.WAC,
            price = bd("150"),
            methodData = WacMethodData(bd("150000"), 1)
        )

        // Today's trades
        val today = LocalDate.of(2025, 1, 20)
        tradeRepo.metrics["BOOK1#GOLDMAN#AAPL|2025-01-20|TRADE_DATE"] = TradeMetrics(
            netQuantity = 500,
            grossLong = 500,
            grossShort = 0,
            tradeCount = 1,
            totalNotional = bd("80000"),
            lastSequenceNum = 2,
            lastTradeTime = Instant.now()
        )
        tradeRepo.trades["BOOK1#GOLDMAN#AAPL|2025-01-20|TRADE_DATE"] = listOf(
            TradeRecord(2, 500, bd("160"))
        )

        val request = PositionCalcRequest(
            requestId = "test-2",
            positionId = 1,
            positionKey = "BOOK1#GOLDMAN#AAPL",
            dateBasis = DateBasis.TRADE_DATE,
            businessDate = today,
            priceMethods = setOf(PriceCalculationMethod.WAC),
            triggeringTradeSequence = 2
        )

        engine.calculatePosition(request)

        val saved = snapshotRepo.savedSnapshots.last()
        assertEquals(1500L, saved.netQuantity) // 1000 + 500
        assertEquals(1500L, saved.grossLong)
        assertEquals(CalculationMethod.INCREMENTAL, saved.calculationMethod)

        val savedPrice = priceRepo.savedPrices.last()
        assertEquals(bd("153.333333333333"), savedPrice.price)
    }

    @Test
    fun `no trades for date creates no snapshot`() {
        val request = PositionCalcRequest(
            requestId = "test-3",
            positionId = 1,
            positionKey = "BOOK1#GOLDMAN#AAPL",
            dateBasis = DateBasis.TRADE_DATE,
            businessDate = LocalDate.of(2025, 1, 20),
            priceMethods = setOf(PriceCalculationMethod.WAC),
            triggeringTradeSequence = 1
        )

        engine.calculatePosition(request)

        assertTrue(snapshotRepo.savedSnapshots.isEmpty())
    }

    // -- Fake implementations for unit testing --

    class FakeTradeRepo : PositionTradeRepository() {
        val metrics = mutableMapOf<String, TradeMetrics>()
        val trades = mutableMapOf<String, List<TradeRecord>>()

        override fun aggregateMetrics(positionKey: String, businessDate: LocalDate, dateBasis: DateBasis): TradeMetrics? {
            return metrics["$positionKey|$businessDate|$dateBasis"]
        }

        override fun findTradesByPositionKeyAndDate(positionKey: String, businessDate: LocalDate, dateBasis: DateBasis): List<TradeRecord> {
            return trades["$positionKey|$businessDate|$dateBasis"] ?: emptyList()
        }
    }

    class FakeSnapshotRepo : PositionSnapshotRepository() {
        val snapshots = mutableMapOf<String, PositionSnapshot>()
        val savedSnapshots = mutableListOf<PositionSnapshot>()

        override fun findSnapshot(positionKey: String, businessDate: LocalDate, dateBasis: DateBasis): PositionSnapshot? {
            return snapshots["$positionKey|$businessDate|$dateBasis"]
        }

        override fun saveSnapshot(snapshot: PositionSnapshot, dateBasis: DateBasis, changeReason: ChangeReason) {
            savedSnapshots.add(snapshot)
        }
    }

    class FakePriceRepo : PositionAveragePriceRepository() {
        val prices = mutableMapOf<String, PositionAveragePrice>()
        val savedPrices = mutableListOf<PositionAveragePrice>()

        override fun findPrice(positionKey: String, businessDate: LocalDate, priceMethod: PriceCalculationMethod, dateBasis: DateBasis): PositionAveragePrice? {
            return prices["$positionKey|$businessDate|$priceMethod|$dateBasis"]
        }

        override fun savePrice(price: PositionAveragePrice, dateBasis: DateBasis) {
            savedPrices.add(price)
        }

        override fun findPricesForSnapshot(positionKey: String, businessDate: LocalDate, dateBasis: DateBasis): List<PositionAveragePrice> {
            return prices.filterKeys { it.startsWith("$positionKey|$businessDate|") }.values.toList()
        }
    }
}
