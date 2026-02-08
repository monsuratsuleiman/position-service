package com.positionservice.db

import com.positionservice.config.DatabaseConfig
import com.positionservice.config.DatabaseFactory
import com.positionservice.domain.*
import com.positionservice.domain.ScopeField
import com.positionservice.domain.ScopePredicate
import org.jetbrains.exposed.sql.deleteAll
import org.jetbrains.exposed.sql.transactions.transaction
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import org.testcontainers.containers.PostgreSQLContainer

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("PositionConfigRepository Tests")
class PositionConfigRepositoryTest {

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
    private lateinit var configRepo: PositionConfigRepository

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
        configRepo = PositionConfigRepository()
    }

    @BeforeEach
    fun cleanTables() {
        transaction {
            PositionConfigsTable.deleteAll()
        }
    }

    @AfterAll
    fun teardownAll() {
        dbFactory.close()
    }

    @Nested
    @DisplayName("CRUD Operations")
    inner class CrudOperations {

        @Test
        @DisplayName("Create and find config by ID")
        fun createAndFindById() {
            val config = PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Official Positions",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC)
            )
            val created = configRepo.create(config)

            assertTrue(created.configId > 0)
            assertEquals("Official Positions", created.name)

            val found = configRepo.findById(created.configId)
            assertNotNull(found)
            assertEquals(created.configId, found!!.configId)
            assertEquals(PositionConfigType.OFFICIAL, found.type)
            assertEquals("Official Positions", found.name)
            assertEquals(PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT, found.keyFormat)
            assertEquals(setOf(PriceCalculationMethod.WAC), found.priceMethods)
            assertTrue(found.active)
            assertEquals(ScopePredicate.All, found.scope)
        }

        @Test
        @DisplayName("Find all configs returns all rows")
        fun findAll() {
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

            val all = configRepo.findAll()
            assertEquals(2, all.size)
            assertTrue(all.all { it.scope == ScopePredicate.All })
        }

        @Test
        @DisplayName("Find active configs excludes inactive")
        fun findActive() {
            val c1 = configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Official",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC)
            ))
            configRepo.create(PositionConfig(
                type = PositionConfigType.DESK,
                name = "Desk",
                keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                active = false
            ))

            val active = configRepo.findActive()
            assertEquals(1, active.size)
            assertEquals(c1.configId, active[0].configId)
            assertEquals(ScopePredicate.All, active[0].scope)
        }

        @Test
        @DisplayName("Update config changes fields")
        fun updateConfig() {
            val created = configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Original",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC)
            ))

            configRepo.update(created.copy(
                name = "Updated",
                keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC, PriceCalculationMethod.VWAP)
            ))

            val found = configRepo.findById(created.configId)
            assertNotNull(found)
            assertEquals("Updated", found!!.name)
            assertEquals(PositionKeyFormat.BOOK_INSTRUMENT, found.keyFormat)
            assertEquals(setOf(PriceCalculationMethod.WAC, PriceCalculationMethod.VWAP), found.priceMethods)
            assertEquals(ScopePredicate.All, found.scope)
        }

        @Test
        @DisplayName("Deactivate config sets active to false")
        fun deactivateConfig() {
            val created = configRepo.create(PositionConfig(
                type = PositionConfigType.USER,
                name = "User Config",
                keyFormat = PositionKeyFormat.INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC)
            ))

            configRepo.deactivate(created.configId)

            val found = configRepo.findById(created.configId)
            assertNotNull(found)
            assertFalse(found!!.active)

            val active = configRepo.findActive()
            assertTrue(active.none { it.configId == created.configId })
        }

        @Test
        @DisplayName("Find by non-existent ID returns null")
        fun findByNonExistentId() {
            assertNull(configRepo.findById(999L))
        }

        @Test
        @DisplayName("Multiple price methods are stored and retrieved correctly")
        fun multiplePriceMethods() {
            val created = configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Multi-Price",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC, PriceCalculationMethod.VWAP)
            ))

            val found = configRepo.findById(created.configId)
            assertNotNull(found)
            assertEquals(2, found!!.priceMethods.size)
            assertTrue(found.priceMethods.contains(PriceCalculationMethod.WAC))
            assertTrue(found.priceMethods.contains(PriceCalculationMethod.VWAP))
        }
    }

    @Nested
    @DisplayName("Scope")
    inner class ScopeTests {

        @Test
        @DisplayName("Create and retrieve config with custom scope")
        fun customScope() {
            val config = PositionConfig(
                type = PositionConfigType.DESK,
                name = "Desk Equity Flow",
                keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "EQUITY-FLOW"))
            )
            val created = configRepo.create(config)
            val found = configRepo.findById(created.configId)

            assertNotNull(found)
            assertEquals(ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "EQUITY-FLOW")), found!!.scope)
        }

        @Test
        @DisplayName("Update config scope changes scope value")
        fun updateScope() {
            val created = configRepo.create(PositionConfig(
                type = PositionConfigType.DESK,
                name = "Scope Update Test",
                keyFormat = PositionKeyFormat.BOOK_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "EQUITY-FLOW"))
            ))

            configRepo.update(created.copy(scope = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "EQUITY-DERIV"))))

            val found = configRepo.findById(created.configId)
            assertNotNull(found)
            assertEquals(ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "EQUITY-DERIV")), found!!.scope)
        }

        @Test
        @DisplayName("Composite unique constraint on (config_type, key_format, scope)")
        fun compositeUniqueConstraint() {
            configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Config A",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.All
            ))

            // Same (config_type, key_format, scope) should fail
            assertThrows<Exception> {
                configRepo.create(PositionConfig(
                    type = PositionConfigType.OFFICIAL,
                    name = "Config B",
                    keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                    priceMethods = setOf(PriceCalculationMethod.WAC),
                    scope = ScopePredicate.All
                ))
            }

            // Different scope should succeed
            val different = configRepo.create(PositionConfig(
                type = PositionConfigType.OFFICIAL,
                name = "Config C",
                keyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
                priceMethods = setOf(PriceCalculationMethod.WAC),
                scope = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "EQUITY-FLOW"))
            ))
            assertTrue(different.configId > 0)
        }
    }
}
