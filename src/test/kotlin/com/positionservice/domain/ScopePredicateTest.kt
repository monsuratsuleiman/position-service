package com.positionservice.domain

import org.junit.jupiter.api.*
import org.junit.jupiter.api.Assertions.*
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

@DisplayName("ScopePredicate Tests")
class ScopePredicateTest {

    private fun testTrade(
        book: String = "BOOK1",
        counterparty: String = "GOLDMAN",
        instrument: String = "AAPL",
        source: String = "BLOOMBERG"
    ) = TradeEvent(
        sequenceNum = 1,
        book = book,
        counterparty = counterparty,
        instrument = instrument,
        signedQuantity = 100,
        price = BigDecimal("150.000000"),
        tradeTime = Instant.parse("2026-02-07T10:00:00Z"),
        tradeDate = LocalDate.of(2026, 2, 7),
        settlementDate = LocalDate.of(2026, 2, 9),
        source = source,
        sourceId = "BLM-1"
    )

    @Nested
    @DisplayName("All predicate")
    inner class AllPredicate {

        @Test
        @DisplayName("All matches any trade")
        fun matchesAnyTrade() {
            assertTrue(ScopePredicate.All.matches(testTrade()))
            assertTrue(ScopePredicate.All.matches(testTrade(book = "OTHER", instrument = "TSLA")))
        }
    }

    @Nested
    @DisplayName("Criteria predicate")
    inner class CriteriaPredicate {

        @Test
        @DisplayName("Single BOOK criterion matches when book equals")
        fun singleBookCriterion() {
            val predicate = ScopePredicate.Criteria(mapOf(ScopeField.BOOK to "BOOK1"))
            assertTrue(predicate.matches(testTrade(book = "BOOK1")))
            assertFalse(predicate.matches(testTrade(book = "BOOK2")))
        }

        @Test
        @DisplayName("Single COUNTERPARTY criterion matches correctly")
        fun singleCounterpartyCriterion() {
            val predicate = ScopePredicate.Criteria(mapOf(ScopeField.COUNTERPARTY to "GOLDMAN"))
            assertTrue(predicate.matches(testTrade(counterparty = "GOLDMAN")))
            assertFalse(predicate.matches(testTrade(counterparty = "JPMC")))
        }

        @Test
        @DisplayName("Single INSTRUMENT criterion matches correctly")
        fun singleInstrumentCriterion() {
            val predicate = ScopePredicate.Criteria(mapOf(ScopeField.INSTRUMENT to "AAPL"))
            assertTrue(predicate.matches(testTrade(instrument = "AAPL")))
            assertFalse(predicate.matches(testTrade(instrument = "TSLA")))
        }

        @Test
        @DisplayName("Single SOURCE criterion matches correctly")
        fun singleSourceCriterion() {
            val predicate = ScopePredicate.Criteria(mapOf(ScopeField.SOURCE to "BLOOMBERG"))
            assertTrue(predicate.matches(testTrade(source = "BLOOMBERG")))
            assertFalse(predicate.matches(testTrade(source = "REUTERS")))
        }

        @Test
        @DisplayName("Multi-criteria uses AND semantics - all must match")
        fun multiCriteriaAndSemantics() {
            val predicate = ScopePredicate.Criteria(mapOf(
                ScopeField.BOOK to "BOOK1",
                ScopeField.INSTRUMENT to "AAPL"
            ))
            // Both match
            assertTrue(predicate.matches(testTrade(book = "BOOK1", instrument = "AAPL")))
            // Only book matches
            assertFalse(predicate.matches(testTrade(book = "BOOK1", instrument = "TSLA")))
            // Only instrument matches
            assertFalse(predicate.matches(testTrade(book = "BOOK2", instrument = "AAPL")))
            // Neither matches
            assertFalse(predicate.matches(testTrade(book = "BOOK2", instrument = "TSLA")))
        }

        @Test
        @DisplayName("Empty criteria map matches everything")
        fun emptyCriteriaMatchesAll() {
            val predicate = ScopePredicate.Criteria(emptyMap())
            assertTrue(predicate.matches(testTrade()))
            assertTrue(predicate.matches(testTrade(book = "ANY", counterparty = "ANY", instrument = "ANY")))
        }
    }

    @Nested
    @DisplayName("ScopeField extraction")
    inner class ScopeFieldExtraction {

        @Test
        @DisplayName("Each ScopeField extracts the correct trade field")
        fun extractionCorrectness() {
            val trade = testTrade(book = "B1", counterparty = "C1", instrument = "I1", source = "S1")
            assertEquals("B1", ScopeField.BOOK.extractFrom(trade))
            assertEquals("C1", ScopeField.COUNTERPARTY.extractFrom(trade))
            assertEquals("I1", ScopeField.INSTRUMENT.extractFrom(trade))
            assertEquals("S1", ScopeField.SOURCE.extractFrom(trade))
        }
    }
}
