package com.positionservice.e2e

import com.positionservice.calculation.PositionCalculationEngine
import com.positionservice.config.DatabaseConfig
import com.positionservice.config.DatabaseFactory
import com.positionservice.db.*
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.*
import org.testcontainers.containers.PostgreSQLContainer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class BaseIntegrationTest {

    companion object {
        val postgres: PostgreSQLContainer<*> = PostgreSQLContainer("postgres:15-alpine")
            .withDatabaseName("position_service_test")
            .withUsername("test")
            .withPassword("test")

        init {
            postgres.start()
        }
    }

    protected lateinit var dbFactory: DatabaseFactory
    protected lateinit var tradeRepo: PositionTradeRepository
    protected lateinit var positionKeyRepo: PositionKeyRepository
    protected lateinit var snapshotRepo: PositionSnapshotRepository
    protected lateinit var priceRepo: PositionAveragePriceRepository
    protected lateinit var configRepo: PositionConfigRepository
    protected lateinit var engine: PositionCalculationEngine

    @BeforeAll
    fun setupAll() {
        if (!postgres.isRunning) {
            postgres.start()
        }

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
        snapshotRepo = PositionSnapshotRepository()
        priceRepo = PositionAveragePriceRepository()
        configRepo = PositionConfigRepository()
        engine = PositionCalculationEngine(tradeRepo, snapshotRepo, priceRepo)
    }

    @AfterEach
    fun cleanTables() {
        transaction {
            // Truncate in FK-safe order: prices before snapshots
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
    }

    @AfterAll
    fun teardownAll() {
        dbFactory.close()
    }
}
