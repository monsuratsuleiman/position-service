package com.positionservice.consumer

import com.positionservice.config.DatabaseConfig
import com.positionservice.config.DatabaseFactory
import com.positionservice.db.*
import com.positionservice.domain.*
import com.positionservice.domain.ScopeField
import com.positionservice.domain.ScopePredicate
import com.positionservice.kafka.KafkaProducerWrapper
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.PostgreSQLContainer
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.time.LocalDate

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("TradeEventConsumer - Multi-Config Tests")
class TradeEventConsumerTest {

    companion object {
        val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:15-alpine")
            .withDatabaseName("position_service_test")
            .withUsername("test")
            .withPassword("test")

        init {
            postgres.start()
        }
    }

    private lateinit var dbFactory: DatabaseFactory
    private lateinit var tradeRepo: PositionTradeRepository
    private lateinit var positionKeyRepo: PositionKeyRepository
    private lateinit var configRepo: PositionConfigRepository
    private lateinit var capturedRequests: MutableList<PositionCalcRequest>
    private lateinit var consumer: TradeEventConsumer

    private fun bd(value: String): BigDecimal = BigDecimal(value).setScale(6, RoundingMode.HALF_UP)

    @BeforeAll
    fun setupAll() {
        val dbConfig = DatabaseConfig(
            url = postgres.jdbcUrl,
            user = postgres.username,
            password = postgres.password,
            maxPoolSize = 5
        )
        dbFactory = DatabaseFactory(dbConfig)
        dbFactory.connect()

        tradeRepo = PositionTradeRepository()
        positionKeyRepo = PositionKeyRepository()
        configRepo = PositionConfigRepository()
    }

    @BeforeEach
    fun setup() {
        // Clean tables
        transaction {
            PositionAveragePricesTable.deleteAll()
            PositionAveragePricesSettledTable.deleteAll()
            PositionSnapshotsHistoryTable.deleteAll()
            PositionSnapshotsSettledHistoryTable.deleteAll()
            PositionSnapshotsTable.deleteAll()
            PositionSnapshotsSettledTable.deleteAll()
            PositionTradesTable.deleteAll()
            PositionKeysTable.deleteAll()
            PositionConfigsTable.deleteAll()
        }

        capturedRequests = mutableListOf()

        // Use a capturing Kafka producer (subclass that captures instead of sending)
        val capturingProducer = CapturingKafkaProducer(capturedRequests)
        consumer = TradeEventConsumer(tradeRepo, positionKeyRepo, capturingProducer, configRepo)
    }

    @AfterAll
    fun teardownAll() {
        dbFactory.close()
    }

    @Test
    @DisplayName("Single config produces 2 calc requests (trade + settlement)")
    fun singleConfigProducesTwoRequests() {
        // Seed one config
        configRepo.create(PositionConfig(
            type = PositionConfigType.OFFICIAL,
            name = "Official",
            keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))

        val trade = TradeEvent(
            sequenceNum = 1,
            book = "BOOK1",
            counterparty = "GOLDMAN",
            instrument = "AAPL",
            signedQuantity = 100,
            price = bd("150.000000"),
            tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
            tradeDate = LocalDate.of(2026, 2, 7),
            settlementDate = LocalDate.of(2026, 2, 9),
            source = "BLOOMBERG",
            sourceId = "BLM-1"
        )
        consumer.processTrade(trade)

        assertEquals(2, capturedRequests.size, "Should produce 2 calc requests (trade + settlement)")
        assertEquals(DateBasis.TRADE_DATE, capturedRequests[0].dateBasis)
        assertEquals(DateBasis.SETTLEMENT_DATE, capturedRequests[1].dateBasis)
        assertEquals("BOOK1#GOLDMAN#AAPL", capturedRequests[0].positionKey)
        assertEquals(PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT, capturedRequests[0].keyFormat)
    }

    @Test
    @DisplayName("Three configs produce 6 calc requests (3 configs x 2 date bases)")
    fun multiConfigProducesSixRequests() {
        // Seed three configs
        configRepo.create(PositionConfig(
            type = PositionConfigType.OFFICIAL,
            name = "Official",
            keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))
        configRepo.create(PositionConfig(
            type = PositionConfigType.DESK,
            name = "Desk Positions",
            keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))
        configRepo.create(PositionConfig(
            type = PositionConfigType.USER,
            name = "User Positions",
            keyFormat = PositionKeyFormat.INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))

        val trade = TradeEvent(
            sequenceNum = 1,
            book = "BOOK1",
            counterparty = "GOLDMAN",
            instrument = "AAPL",
            signedQuantity = 100,
            price = bd("150.000000"),
            tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
            tradeDate = LocalDate.of(2026, 2, 7),
            settlementDate = LocalDate.of(2026, 2, 9),
            source = "BLOOMBERG",
            sourceId = "BLM-1"
        )
        consumer.processTrade(trade)

        assertEquals(6, capturedRequests.size, "3 configs x 2 date bases = 6 calc requests")

        // Verify keys generated per config
        val keys = capturedRequests.map { it.positionKey }.toSet()
        assertTrue(keys.contains("BOOK1#GOLDMAN#AAPL"), "Should have BOOK_COUNTERPARTY_INSTRUMENT key")
        assertTrue(keys.contains("BOOK1#AAPL"), "Should have BOOK_INSTRUMENT key")
        assertTrue(keys.contains("AAPL"), "Should have INSTRUMENT key")

        // Verify keyFormat is set correctly on each request
        val bciRequests = capturedRequests.filter { it.positionKey == "BOOK1#GOLDMAN#AAPL" }
        assertEquals(2, bciRequests.size)
        assertTrue(bciRequests.all { it.keyFormat == PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT })

        val biRequests = capturedRequests.filter { it.positionKey == "BOOK1#AAPL" }
        assertEquals(2, biRequests.size)
        assertTrue(biRequests.all { it.keyFormat == PositionKeyFormat.BOOK_INSTRUMENT })

        val iRequests = capturedRequests.filter { it.positionKey == "AAPL" }
        assertEquals(2, iRequests.size)
        assertTrue(iRequests.all { it.keyFormat == PositionKeyFormat.INSTRUMENT })
    }

    @Test
    @DisplayName("Trade is stored once with canonical key regardless of config count")
    fun tradeStoredOnceCanonically() {
        configRepo.create(PositionConfig(
            type = PositionConfigType.OFFICIAL,
            name = "Official",
            keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))
        configRepo.create(PositionConfig(
            type = PositionConfigType.DESK,
            name = "Desk",
            keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))

        val trade = TradeEvent(
            sequenceNum = 1,
            book = "BOOK1",
            counterparty = "GOLDMAN",
            instrument = "AAPL",
            signedQuantity = 100,
            price = bd("150.000000"),
            tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
            tradeDate = LocalDate.of(2026, 2, 7),
            settlementDate = LocalDate.of(2026, 2, 9),
            source = "BLOOMBERG",
            sourceId = "BLM-1"
        )
        consumer.processTrade(trade)

        // Verify trade stored once
        val trades = tradeRepo.findTradesByPositionKeyAndDate(
            "BOOK1#GOLDMAN#AAPL",
            LocalDate.of(2026, 2, 7),
            DateBasis.TRADE_DATE
        )
        assertEquals(1, trades.size, "Trade should be stored exactly once")

        // Verify position_keys created for each config
        val positionKeys = transaction {
            PositionKeysTable.selectAll().toList()
        }
        assertEquals(2, positionKeys.size, "Should have 2 position_keys (one per config)")
    }

    @Test
    @DisplayName("Duplicate trade is idempotent across multiple configs")
    fun duplicateTradeIdempotent() {
        configRepo.create(PositionConfig(
            type = PositionConfigType.OFFICIAL,
            name = "Official",
            keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))

        val trade = TradeEvent(
            sequenceNum = 1,
            book = "BOOK1",
            counterparty = "GOLDMAN",
            instrument = "AAPL",
            signedQuantity = 100,
            price = bd("150.000000"),
            tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
            tradeDate = LocalDate.of(2026, 2, 7),
            settlementDate = LocalDate.of(2026, 2, 9),
            source = "BLOOMBERG",
            sourceId = "BLM-1"
        )
        consumer.processTrade(trade)
        assertEquals(2, capturedRequests.size)

        // Process same trade again
        consumer.processTrade(trade)
        assertEquals(2, capturedRequests.size, "Duplicate trade should not produce additional requests")
    }

    @Test
    @DisplayName("Inactive configs are ignored")
    fun inactiveConfigsIgnored() {
        configRepo.create(PositionConfig(
            type = PositionConfigType.OFFICIAL,
            name = "Official",
            keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC)
        ))
        configRepo.create(PositionConfig(
            type = PositionConfigType.DESK,
            name = "Inactive Desk",
            keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
            priceMethods = setOf(PriceCalculationMethod.WAC),
            active = false
        ))

        val trade = TradeEvent(
            sequenceNum = 1,
            book = "BOOK1",
            counterparty = "GOLDMAN",
            instrument = "AAPL",
            signedQuantity = 100,
            price = bd("150.000000"),
            tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
            tradeDate = LocalDate.of(2026, 2, 7),
            settlementDate = LocalDate.of(2026, 2, 9),
            source = "BLOOMBERG",
            sourceId = "BLM-1"
        )
        consumer.processTrade(trade)

        assertEquals(2, capturedRequests.size, "Only active config should produce requests")
        assertTrue(capturedRequests.all { it.positionKey == "BOOK1#GOLDMAN#AAPL" })
    }

    @Nested
    @DisplayName("Scope Filtering")
    inner class ScopeFiltering {

        @Test
        @DisplayName("BOOK scope: only produces requests for matching book")
        fun bookScopeFilter() {
            configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Book1 Only",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "BOOK1"))
            ))

            // Trade with matching book
            val trade1 = TradeEvent(
                sequenceNum = 100,
                book = "BOOK1",
                counterparty = "GOLDMAN",
                instrument = "AAPL",
                signedQuantity = 100,
                price = bd("150.000000"),
                tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
                tradeDate = LocalDate.of(2026, 2, 7),
                settlementDate = LocalDate.of(2026, 2, 9),
                source = "BLOOMBERG",
                sourceId = "BLM-100"
            )
            consumer.processTrade(trade1)
            assertEquals(2, capturedRequests.size, "Matching trade should produce 2 requests")

            capturedRequests.clear()

            // Trade with non-matching book
            val trade2 = TradeEvent(
                sequenceNum = 101,
                book = "BOOK2",
                counterparty = "GOLDMAN",
                instrument = "AAPL",
                signedQuantity = 50,
                price = bd("155.000000"),
                tradeTime = Instant.parse("2026-02-07T11:00:00Z"),
                tradeDate = LocalDate.of(2026, 2, 7),
                settlementDate = LocalDate.of(2026, 2, 9),
                source = "BLOOMBERG",
                sourceId = "BLM-101"
            )
            consumer.processTrade(trade2)
            assertEquals(0, capturedRequests.size, "Non-matching trade should produce 0 requests")
        }

        @Test
        @DisplayName("Multi-criteria: requires both BOOK and INSTRUMENT to match")
        fun multiCriteriaFilter() {
            configRepo.create(PositionConfig(
                type = PositionConfigType.DESK,
                name = "Book1 AAPL Only",
                keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(
                    ScopeField.BOOK to "BOOK1",
                    ScopeField.INSTRUMENT to "AAPL"
                ))
            ))

            // Both match
            val trade1 = TradeEvent(
                sequenceNum = 200,
                book = "BOOK1",
                counterparty = "GOLDMAN",
                instrument = "AAPL",
                signedQuantity = 100,
                price = bd("150.000000"),
                tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
                tradeDate = LocalDate.of(2026, 2, 7),
                settlementDate = LocalDate.of(2026, 2, 9),
                source = "BLOOMBERG",
                sourceId = "BLM-200"
            )
            consumer.processTrade(trade1)
            assertEquals(2, capturedRequests.size, "Both criteria match => 2 requests")

            capturedRequests.clear()

            // Only book matches
            val trade2 = TradeEvent(
                sequenceNum = 201,
                book = "BOOK1",
                counterparty = "GOLDMAN",
                instrument = "TSLA",
                signedQuantity = 50,
                price = bd("200.000000"),
                tradeTime = Instant.parse("2026-02-07T11:00:00Z"),
                tradeDate = LocalDate.of(2026, 2, 7),
                settlementDate = LocalDate.of(2026, 2, 9),
                source = "BLOOMBERG",
                sourceId = "BLM-201"
            )
            consumer.processTrade(trade2)
            assertEquals(0, capturedRequests.size, "Only book matches => 0 requests")
        }

        @Test
        @DisplayName("Mixed: ALL config matches everything, CRITERIA config matches subset")
        fun mixedConfigs() {
            configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "All Trades",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.All
            ))
            configRepo.create(PositionConfig(
                type = PositionConfigType.DESK,
                name = "Book1 Only",
                keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "BOOK1"))
            ))

            // Trade matching both configs
            val trade1 = TradeEvent(
                sequenceNum = 300,
                book = "BOOK1",
                counterparty = "GOLDMAN",
                instrument = "AAPL",
                signedQuantity = 100,
                price = bd("150.000000"),
                tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
                tradeDate = LocalDate.of(2026, 2, 7),
                settlementDate = LocalDate.of(2026, 2, 9),
                source = "BLOOMBERG",
                sourceId = "BLM-300"
            )
            consumer.processTrade(trade1)
            assertEquals(4, capturedRequests.size, "2 configs x 2 date bases = 4 requests")

            capturedRequests.clear()

            // Trade matching only ALL config
            val trade2 = TradeEvent(
                sequenceNum = 301,
                book = "BOOK2",
                counterparty = "JPMC",
                instrument = "TSLA",
                signedQuantity = 200,
                price = bd("200.000000"),
                tradeTime = Instant.parse("2026-02-07T11:00:00Z"),
                tradeDate = LocalDate.of(2026, 2, 7),
                settlementDate = LocalDate.of(2026, 2, 9),
                source = "BLOOMBERG",
                sourceId = "BLM-301"
            )
            consumer.processTrade(trade2)
            assertEquals(2, capturedRequests.size, "Only ALL config matches => 2 requests")
            assertTrue(capturedRequests.all { it.positionKey == "BOOK2#JPMC#TSLA" })
        }
    }
}

/**
 * A test-only Kafka producer wrapper that captures requests instead of actually sending to Kafka.
 */
class CapturingKafkaProducer(
    private val captured: MutableList<PositionCalcRequest>
) : KafkaProducerWrapper(com.positionservice.config.KafkaConfig("", "", "", "")) {

    override fun publishCalcRequest(request: PositionCalcRequest) {
        captured.add(request)
    }
}
