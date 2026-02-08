package com.positionservice.domain

import kotlinx.serialization.Serializable

@Serializable
enum class PositionKeyFormat {
    BOOK_COUNTERPARTY_INSTRUMENT,
    BOOK_INSTRUMENT,
    COUNTERPARTY_INSTRUMENT,
    INSTRUMENT,
    BOOK;

    fun generateKey(book: String, counterparty: String, instrument: String): String = when (this) {
        BOOK_COUNTERPARTY_INSTRUMENT -> "$book#$counterparty#$instrument"
        BOOK_INSTRUMENT -> "$book#$instrument"
        COUNTERPARTY_INSTRUMENT -> "$counterparty#$instrument"
        INSTRUMENT -> instrument
        BOOK -> book
    }

    fun parseDimensions(positionKey: String): Map<String, String> {
        val parts = positionKey.split("#")
        return when (this) {
            BOOK_COUNTERPARTY_INSTRUMENT -> mapOf("book" to parts[0], "counterparty" to parts[1], "instrument" to parts[2])
            BOOK_INSTRUMENT -> mapOf("book" to parts[0], "instrument" to parts[1])
            COUNTERPARTY_INSTRUMENT -> mapOf("counterparty" to parts[0], "instrument" to parts[1])
            INSTRUMENT -> mapOf("instrument" to parts[0])
            BOOK -> mapOf("book" to parts[0])
        }
    }

    fun extractDimensions(book: String, counterparty: String, instrument: String): Triple<String?, String?, String?> = when (this) {
        BOOK_COUNTERPARTY_INSTRUMENT -> Triple(book, counterparty, instrument)
        BOOK_INSTRUMENT -> Triple(book, null, instrument)
        COUNTERPARTY_INSTRUMENT -> Triple(null, counterparty, instrument)
        INSTRUMENT -> Triple(null, null, instrument)
        BOOK -> Triple(book, null, null)
    }
}

@Serializable
enum class PositionConfigType { OFFICIAL, USER, DESK }

@Serializable
enum class PriceCalculationMethod { WAC, VWAP, FIFO, LIFO }

@Serializable
enum class DateBasis { TRADE_DATE, SETTLEMENT_DATE }

@Serializable
enum class CalculationMethod { INCREMENTAL, FULL_RECALC }

@Serializable
enum class ChangeReason { INITIAL, LATE_TRADE, CORRECTION }

@Serializable
enum class ScopeField {
    BOOK {
        override fun extractFrom(trade: TradeEvent): String = trade.book
    },
    COUNTERPARTY {
        override fun extractFrom(trade: TradeEvent): String = trade.counterparty
    },
    INSTRUMENT {
        override fun extractFrom(trade: TradeEvent): String = trade.instrument
    },
    SOURCE {
        override fun extractFrom(trade: TradeEvent): String = trade.source
    };

    abstract fun extractFrom(trade: TradeEvent): String
}
