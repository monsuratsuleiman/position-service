package com.positionservice.db

import com.positionservice.e2e.BaseIntegrationTest
import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.time.LocalDate

@DisplayName("PositionKeyRepository Search Tests")
class PositionKeyRepositoryTest : BaseIntegrationTest() {

    private val tradeDate = LocalDate.of(2025, 1, 20)
    private val settlementDate = LocalDate.of(2025, 1, 22)

    private fun seedPositionKey(
        positionKey: String,
        configId: Long = 1L,
        configType: String = "OFFICIAL",
        configName: String = "Official Positions",
        book: String? = null,
        counterparty: String? = null,
        instrument: String? = null,
        sequenceNum: Long = 1L
    ) {
        positionKeyRepo.upsertPositionKey(
            positionKey = positionKey,
            configId = configId,
            configType = configType,
            configName = configName,
            book = book,
            counterparty = counterparty,
            instrument = instrument,
            tradeDate = tradeDate,
            settlementDate = settlementDate,
            sequenceNum = sequenceNum
        )
    }

    private fun seedTestData() {
        // BCI keys — all dimensions set
        seedPositionKey("BOOK1#GSCO#AAPL", book = "BOOK1", counterparty = "GSCO", instrument = "AAPL", sequenceNum = 1)
        seedPositionKey("BOOK1#MSCO#TSLA", book = "BOOK1", counterparty = "MSCO", instrument = "TSLA", sequenceNum = 2)
        seedPositionKey("BOOK2#GSCO#AAPL", book = "BOOK2", counterparty = "GSCO", instrument = "AAPL", sequenceNum = 3)

        // BOOK_INSTRUMENT key — counterparty is null
        seedPositionKey("BOOK1#AAPL", configId = 2L, configName = "Desk Positions",
            book = "BOOK1", counterparty = null, instrument = "AAPL", sequenceNum = 4)

        // INSTRUMENT key — book and counterparty are null
        seedPositionKey("GOOGL", configId = 3L, configName = "Instrument View",
            book = null, counterparty = null, instrument = "GOOGL", sequenceNum = 5)

        // BOOK key — counterparty and instrument are null
        seedPositionKey("BOOK2", configId = 4L, configName = "Book View",
            book = "BOOK2", counterparty = null, instrument = null, sequenceNum = 6)
    }

    @Nested
    @DisplayName("search()")
    inner class Search {

        @Test
        @DisplayName("No filters returns all position keys")
        fun noFilters() {
            seedTestData()
            val results = positionKeyRepo.search()
            assertEquals(6, results.size)
        }

        @Test
        @DisplayName("Filter by book returns matching rows")
        fun filterByBook() {
            seedTestData()
            val results = positionKeyRepo.search(book = "BOOK1")
            assertEquals(3, results.size)
            assertTrue(results.all { it.book == "BOOK1" })
        }

        @Test
        @DisplayName("Filter by counterparty returns matching rows")
        fun filterByCounterparty() {
            seedTestData()
            val results = positionKeyRepo.search(counterparty = "GSCO")
            assertEquals(2, results.size)
            assertTrue(results.all { it.counterparty == "GSCO" })
        }

        @Test
        @DisplayName("Filter by instrument returns matching rows")
        fun filterByInstrument() {
            seedTestData()
            val results = positionKeyRepo.search(instrument = "AAPL")
            assertEquals(3, results.size)
            assertTrue(results.all { it.instrument == "AAPL" })
        }

        @Test
        @DisplayName("Combined filters narrow results")
        fun combinedFilters() {
            seedTestData()
            val results = positionKeyRepo.search(book = "BOOK1", instrument = "AAPL")
            assertEquals(2, results.size)
            assertTrue(results.all { it.book == "BOOK1" && it.instrument == "AAPL" })
        }

        @Test
        @DisplayName("No matches returns empty list")
        fun noMatches() {
            seedTestData()
            val results = positionKeyRepo.search(book = "NONEXISTENT")
            assertTrue(results.isEmpty())
        }

        @Test
        @DisplayName("Limit caps result count")
        fun limitCaps() {
            seedTestData()
            val results = positionKeyRepo.search(limit = 2)
            assertEquals(2, results.size)
        }

        @Test
        @DisplayName("Results are ordered by positionKey")
        fun orderedResults() {
            seedTestData()
            val results = positionKeyRepo.search()
            val keys = results.map { it.positionKey }
            assertEquals(keys.sorted(), keys)
        }
    }

    @Nested
    @DisplayName("Distinct dimension queries")
    inner class DistinctDimensions {

        @Test
        @DisplayName("distinctBooks returns sorted unique non-null books")
        fun distinctBooks() {
            seedTestData()
            val books = positionKeyRepo.distinctBooks()
            assertEquals(listOf("BOOK1", "BOOK2"), books)
        }

        @Test
        @DisplayName("distinctCounterparties returns sorted unique non-null counterparties")
        fun distinctCounterparties() {
            seedTestData()
            val cptys = positionKeyRepo.distinctCounterparties()
            assertEquals(listOf("GSCO", "MSCO"), cptys)
        }

        @Test
        @DisplayName("distinctInstruments returns sorted unique non-null instruments")
        fun distinctInstruments() {
            seedTestData()
            val instruments = positionKeyRepo.distinctInstruments()
            assertEquals(listOf("AAPL", "GOOGL", "TSLA"), instruments)
        }

        @Test
        @DisplayName("Rows with null book are excluded from distinctBooks")
        fun nullBooksExcluded() {
            // Only insert keys with null books
            seedPositionKey("GOOGL", configId = 3L, book = null, counterparty = null, instrument = "GOOGL")
            val books = positionKeyRepo.distinctBooks()
            assertTrue(books.isEmpty())
        }
    }
}
