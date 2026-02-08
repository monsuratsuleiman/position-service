package com.positionservice.e2e

import com.positionservice.api.*
import com.positionservice.db.PositionAveragePriceRepository
import com.positionservice.db.PositionConfigRepository
import com.positionservice.db.PositionSnapshotRepository
import com.positionservice.domain.*
import com.positionservice.e2e.TestDataSeeder.bd
import io.ktor.client.call.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*
import io.ktor.http.*
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.config.*
import io.ktor.server.plugins.contentnegotiation.ContentNegotiation
import io.ktor.server.plugins.statuspages.*
import io.ktor.server.application.*
import io.ktor.server.response.*
import io.ktor.server.testing.*
import kotlinx.serialization.json.Json
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.time.LocalDate

@DisplayName("E2E API Tests")
class E2EApiTest : BaseIntegrationTest() {

    private val testJson = Json {
        prettyPrint = true
        encodeDefaults = true
        ignoreUnknownKeys = true
    }

    private fun configureApp(app: Application) {
        app.install(ContentNegotiation) {
            json(testJson)
        }
        app.install(StatusPages) {
            exception<Throwable> { call, cause ->
                call.respond(
                    HttpStatusCode.InternalServerError,
                    mapOf("error" to (cause.message ?: "Internal error"))
                )
            }
        }
        app.configureRoutes(snapshotRepo, priceRepo, configRepo, positionKeyRepo)
    }

    private fun apiTest(block: suspend ApplicationTestBuilder.() -> Unit) = testApplication {
        environment {
            config = MapApplicationConfig()
        }
        application { configureApp(this) }
        block()
    }

    private fun ApplicationTestBuilder.createJsonClient() = createClient {
        install(io.ktor.client.plugins.contentnegotiation.ContentNegotiation) {
            json(Json { ignoreUnknownKeys = true })
        }
    }

    @Nested
    @DisplayName("Health Check")
    inner class HealthCheck {

        @Test
        @DisplayName("E2E-API-001: GET /health returns 200 with status UP")
        fun healthCheck() = apiTest {
            val client = createJsonClient()

            val response = client.get("/api/v1/health")

            assertEquals(HttpStatusCode.OK, response.status)
            val body = response.body<Map<String, String>>()
            assertEquals("UP", body["status"])
        }
    }

    @Nested
    @DisplayName("Get Position by Key and Date")
    inner class GetPositionByKeyAndDate {

        @Test
        @DisplayName("E2E-API-002: GET position by key+date (trade date) returns correct snapshot and prices")
        fun getPositionTradeDate() = apiTest {
            val client = createJsonClient()

            // Seed: insert a trade and run calculation
            val trade = TestDataSeeder.createTradeEvent(
                seq = 1,
                qty = 1000,
                price = bd("150.000000"),
                tradeDate = LocalDate.of(2025, 1, 20),
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade)

            val response = client.get("/api/v1/positions/BOOK1%23GOLDMAN%23AAPL/2025-01-20?dateBasis=TRADE_DATE")

            assertEquals(HttpStatusCode.OK, response.status)
            val position = response.body<PositionResponse>()
            assertEquals("BOOK1#GOLDMAN#AAPL", position.positionKey)
            assertEquals("2025-01-20", position.businessDate)
            assertEquals(1000L, position.netQuantity)
            assertEquals(1000L, position.grossLong)
            assertEquals(0L, position.grossShort)
            assertEquals(1, position.tradeCount)
            assertEquals(1, position.calculationVersion)
            assertTrue(position.prices.isNotEmpty())
            assertEquals("WAC", position.prices[0].method)
            assertEquals(0, bd("150.000000").compareTo(bd(position.prices[0].price)))
        }

        @Test
        @DisplayName("E2E-API-003: GET position by key+date (settlement date) uses settled tables")
        fun getPositionSettlementDate() = apiTest {
            val client = createJsonClient()

            // Seed: insert a trade and run calculation (both date bases)
            val trade = TestDataSeeder.createTradeEvent(
                seq = 2,
                qty = 500,
                price = bd("200.000000"),
                tradeDate = LocalDate.of(2025, 1, 20),
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade)

            val response = client.get("/api/v1/positions/BOOK1%23GOLDMAN%23AAPL/2025-01-22?dateBasis=SETTLEMENT_DATE")

            assertEquals(HttpStatusCode.OK, response.status)
            val position = response.body<PositionResponse>()
            assertEquals("BOOK1#GOLDMAN#AAPL", position.positionKey)
            assertEquals("2025-01-22", position.businessDate)
            assertEquals("SETTLEMENT_DATE", position.dateBasis)
            assertEquals(500L, position.netQuantity)
            assertEquals(500L, position.grossLong)
            assertEquals(0L, position.grossShort)
            assertEquals(1, position.tradeCount)
        }

        @Test
        @DisplayName("E2E-API-004: GET position not found returns 404")
        fun getPositionNotFound() = apiTest {
            val client = createJsonClient()

            val response = client.get("/api/v1/positions/NONEXISTENT%23KEY/2025-01-20?dateBasis=TRADE_DATE")

            assertEquals(HttpStatusCode.NotFound, response.status)
            val body = response.body<Map<String, String>>()
            assertEquals("Position not found", body["error"])
        }
    }

    @Nested
    @DisplayName("Get Position Time Series")
    inner class GetPositionTimeSeries {

        @Test
        @DisplayName("E2E-API-005: GET position time series returns ordered array of dates")
        fun getPositionTimeSeries() = apiTest {
            val client = createJsonClient()

            // Seed 3 trades on different trade dates
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 10,
                qty = 100,
                price = bd("100.000000"),
                tradeDate = LocalDate.of(2025, 1, 20),
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 11,
                qty = 200,
                price = bd("110.000000"),
                tradeDate = LocalDate.of(2025, 1, 21),
                settlementDate = LocalDate.of(2025, 1, 23)
            )
            val trade3 = TestDataSeeder.createTradeEvent(
                seq = 12,
                qty = 300,
                price = bd("120.000000"),
                tradeDate = LocalDate.of(2025, 1, 22),
                settlementDate = LocalDate.of(2025, 1, 24)
            )

            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade1)
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade2)
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade3)

            val response = client.get(
                "/api/v1/positions/BOOK1%23GOLDMAN%23AAPL?dateBasis=TRADE_DATE&from=2025-01-20&to=2025-01-22"
            )

            assertEquals(HttpStatusCode.OK, response.status)
            val summaries = response.body<List<PositionSummaryResponse>>()
            assertEquals(3, summaries.size)
            assertEquals("2025-01-20", summaries[0].businessDate)
            assertEquals("2025-01-21", summaries[1].businessDate)
            assertEquals("2025-01-22", summaries[2].businessDate)
            // Each date has its own trades, verify net quantities match
            assertEquals(100L, summaries[0].netQuantity)
        }
    }

    @Nested
    @DisplayName("Get Position History")
    inner class GetPositionHistory {

        @Test
        @DisplayName("E2E-API-006: GET position history returns version history after re-calculation")
        fun getPositionHistory() = apiTest {
            val client = createJsonClient()

            val businessDate = LocalDate.of(2025, 1, 20)

            // First trade creates version 1
            val trade1 = TestDataSeeder.createTradeEvent(
                seq = 20,
                qty = 100,
                price = bd("150.000000"),
                tradeDate = businessDate,
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade1)

            // Second trade on same date triggers re-calculation (version 2)
            val trade2 = TestDataSeeder.createTradeEvent(
                seq = 21,
                qty = 200,
                price = bd("155.000000"),
                tradeDate = businessDate,
                settlementDate = LocalDate.of(2025, 1, 22)
            )
            TestDataSeeder.insertTradeAndCalc(engine, tradeRepo, positionKeyRepo, trade2)

            val response = client.get(
                "/api/v1/positions/BOOK1%23GOLDMAN%23AAPL/2025-01-20/history?dateBasis=TRADE_DATE"
            )

            assertEquals(HttpStatusCode.OK, response.status)
            val history = response.body<List<HistoryResponse>>()
            assertTrue(history.size >= 2, "Expected at least 2 history entries, got ${history.size}")
            assertEquals(1, history[0].calculationVersion)
            assertEquals(2, history[1].calculationVersion)
            // Version 1 should be superseded
            assertNotNull(history[0].supersededAt)
            // Version 2 should have previous net quantity
            assertEquals(100L, history[1].previousNetQuantity)
            assertEquals(300L, history[1].netQuantity) // 100 + 200
        }
    }

    @Nested
    @DisplayName("Error Handling")
    inner class ErrorHandling {

        @Test
        @DisplayName("E2E-API-007: Invalid date format returns 400")
        fun invalidDateFormat() = apiTest {
            val client = createJsonClient()

            val response = client.get("/api/v1/positions/BOOK1%23GOLDMAN%23AAPL/not-a-date?dateBasis=TRADE_DATE")

            assertEquals(HttpStatusCode.BadRequest, response.status)
            val body = response.body<Map<String, String>>()
            assertEquals("Invalid date format", body["error"])
        }

        @Test
        @DisplayName("E2E-API-008: Invalid dateBasis returns 500 (IllegalArgumentException from valueOf)")
        fun invalidDateBasis() = apiTest {
            val client = createJsonClient()

            val response = client.get("/api/v1/positions/BOOK1%23GOLDMAN%23AAPL/2025-01-20?dateBasis=INVALID")

            // DateBasis.valueOf("INVALID") throws IllegalArgumentException, caught by StatusPages
            assertEquals(HttpStatusCode.InternalServerError, response.status)
            val body = response.body<Map<String, String>>()
            assertNotNull(body["error"])
        }
    }

    @Nested
    @DisplayName("Config CRUD")
    inner class ConfigCrud {

        @Test
        @DisplayName("E2E-API-009: GET /configs returns list with scope")
        fun listConfigs() = apiTest {
            val client = createJsonClient()
            configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Test Config",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "EQUITY-FLOW"))
            ))

            val response = client.get("/api/v1/configs")
            assertEquals(HttpStatusCode.OK, response.status)
            val configs = response.body<List<ConfigResponse>>()
            assertEquals(1, configs.size)
            assertEquals(ScopePredicateDto(type = "CRITERIA", criteria = mapOf("BOOK" to "EQUITY-FLOW")), configs[0].scope)
        }

        @Test
        @DisplayName("E2E-API-010: GET /configs/{id} returns single config with scope")
        fun getConfigById() = apiTest {
            val client = createJsonClient()
            val created = configRepo.create(PositionConfig(
                type = PositionConfigType.DESK,
                name = "Desk Config",
                keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(ScopeField.INSTRUMENT to "AAPL"))
            ))

            val response = client.get("/api/v1/configs/${created.configId}")
            assertEquals(HttpStatusCode.OK, response.status)
            val config = response.body<ConfigResponse>()
            assertEquals(ScopePredicateDto(type = "CRITERIA", criteria = mapOf("INSTRUMENT" to "AAPL")), config.scope)
            assertEquals("Desk Config", config.name)
        }

        @Test
        @DisplayName("E2E-API-011: POST /configs with explicit scope")
        fun createConfigWithScope() = apiTest {
            val client = createJsonClient()

            val response = client.post("/api/v1/configs") {
                contentType(ContentType.Application.Json)
                setBody(ConfigCreateRequest(
                    configType = "OFFICIAL",
                    name = "New Config",
                    keyFormat = "BOOK_COUNTERPARTY_INSTRUMENT",
                    priceMethods = listOf("WAC"),
                    scope = ScopePredicateDto(type = "CRITERIA", criteria = mapOf("BOOK" to "PRIME"))
                ))
            }
            assertEquals(HttpStatusCode.Created, response.status)
            val config = response.body<ConfigResponse>()
            assertEquals(ScopePredicateDto(type = "CRITERIA", criteria = mapOf("BOOK" to "PRIME")), config.scope)
            assertTrue(config.configId > 0)
        }

        @Test
        @DisplayName("E2E-API-012: POST /configs without scope defaults to ALL")
        fun createConfigDefaultScope() = apiTest {
            val client = createJsonClient()

            val response = client.post("/api/v1/configs") {
                contentType(ContentType.Application.Json)
                setBody("""{"configType":"OFFICIAL","name":"Default Scope","keyFormat":"BOOK_COUNTERPARTY_INSTRUMENT","priceMethods":["WAC"]}""")
            }
            assertEquals(HttpStatusCode.Created, response.status)
            val config = response.body<ConfigResponse>()
            assertEquals(ScopePredicateDto(type = "ALL"), config.scope)
        }

        @Test
        @DisplayName("E2E-API-013: PUT /configs/{id} updates scope")
        fun updateConfigScope() = apiTest {
            val client = createJsonClient()
            val created = configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Update Me",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.All
            ))

            val response = client.put("/api/v1/configs/${created.configId}") {
                contentType(ContentType.Application.Json)
                setBody(ConfigUpdateRequest(
                    configType = "OFFICIAL",
                    name = "Updated",
                    keyFormat = "BOOK_COUNTERPARTY_INSTRUMENT",
                    priceMethods = listOf("WAC"),
                    scope = ScopePredicateDto(type = "CRITERIA", criteria = mapOf("BOOK" to "EQUITY-FLOW")),
                    active = true
                ))
            }
            assertEquals(HttpStatusCode.OK, response.status)
            val config = response.body<ConfigResponse>()
            assertEquals(ScopePredicateDto(type = "CRITERIA", criteria = mapOf("BOOK" to "EQUITY-FLOW")), config.scope)
            assertEquals("Updated", config.name)
        }

        @Test
        @DisplayName("E2E-API-014: GET /configs/{id} returns 404 for non-existent config")
        fun getConfigNotFound() = apiTest {
            val client = createJsonClient()

            val response = client.get("/api/v1/configs/999")
            assertEquals(HttpStatusCode.NotFound, response.status)
        }
    }
}
