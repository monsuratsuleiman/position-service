package loadtest

import java.math.BigDecimal
import java.math.RoundingMode
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.random.Random

data class GeneratedTrade(
    val sequenceNum: Long,
    val book: String,
    val counterparty: String,
    val instrument: String,
    val signedQuantity: Long,
    val price: BigDecimal,
    val tradeDate: LocalDate,
    val settlementDate: LocalDate,
    val positionKey: String
) {
    fun toJson(): String {
        val tradeTime = tradeDate.atStartOfDay().plusHours(10).toInstant(ZoneOffset.UTC)
        return """{"sequenceNum":$sequenceNum,"book":"$book","counterparty":"$counterparty","instrument":"$instrument","signedQuantity":$signedQuantity,"price":"${price.toPlainString()}","tradeTime":"$tradeTime","tradeDate":"$tradeDate","settlementDate":"$settlementDate","source":"LOADTEST","sourceId":"LT-$sequenceNum"}"""
    }
}

class TradeGenerator(private val config: LoadTestConfig) {

    companion object {
        private const val SEQ_START = 100_000L
        private const val SEED = 42L

        private val INSTRUMENT_POOL = listOf(
            "AAPL" to BigDecimal("150.000000"),
            "MSFT" to BigDecimal("400.000000"),
            "TSLA" to BigDecimal("250.000000"),
            "GOOGL" to BigDecimal("175.000000"),
            "AMZN" to BigDecimal("185.000000"),
            "META" to BigDecimal("500.000000"),
            "NVDA" to BigDecimal("800.000000"),
            "JPM" to BigDecimal("195.000000"),
            "V" to BigDecimal("280.000000"),
            "JNJ" to BigDecimal("155.000000"),
            "WMT" to BigDecimal("170.000000"),
            "PG" to BigDecimal("160.000000"),
            "UNH" to BigDecimal("520.000000"),
            "HD" to BigDecimal("370.000000"),
            "BAC" to BigDecimal("35.000000"),
            "DIS" to BigDecimal("110.000000"),
            "NFLX" to BigDecimal("620.000000"),
            "ADBE" to BigDecimal("580.000000"),
            "CRM" to BigDecimal("300.000000"),
            "INTC" to BigDecimal("45.000000")
        )

        private val BOOK_POOL = listOf(
            "EQUITY-1", "EQUITY-2", "EQUITY-3", "EQUITY-4", "EQUITY-5",
            "EQUITY-6", "EQUITY-7", "EQUITY-8", "EQUITY-9", "EQUITY-10"
        )

        private val CPTY_POOL = listOf(
            "GSCO", "MSCO", "JPMC", "BOFA", "CITI"
        )
    }

    fun generate(): List<GeneratedTrade> {
        val rng = Random(SEED)
        val instruments = INSTRUMENT_POOL.take(config.instruments)
        val books = BOOK_POOL.take(config.books)
        val cptys = CPTY_POOL.take(config.counterparties)
        val settlementDate = config.businessDate.plusDays(2)

        // Build position keys for deliberate WAC scenario trades
        val positionKeys = mutableListOf<Triple<String, String, String>>() // book, cpty, instrument
        for (book in books) {
            for (cpty in cptys) {
                for (inst in instruments) {
                    positionKeys.add(Triple(book, cpty, inst.first))
                }
            }
        }

        val trades = mutableListOf<GeneratedTrade>()
        var seq = SEQ_START

        // Phase 1: Deliberate WAC scenario trades (3 per position key)
        // This ensures each position key gets predictable buy/buy/sell pattern
        for ((book, cpty, instName) in positionKeys) {
            val basePrice = instruments.first { it.first == instName }.second

            // Trade 1: Buy - first from flat, WAC = trade price
            val price1 = varyPrice(basePrice, rng)
            trades.add(GeneratedTrade(
                sequenceNum = seq++,
                book = book,
                counterparty = cpty,
                instrument = instName,
                signedQuantity = (rng.nextInt(5) + 1) * 100L,
                price = price1,
                tradeDate = config.businessDate,
                settlementDate = settlementDate,
                positionKey = "$book#$cpty#$instName"
            ))

            // Trade 2: Buy more - moving away, weighted average
            val price2 = varyPrice(basePrice, rng)
            trades.add(GeneratedTrade(
                sequenceNum = seq++,
                book = book,
                counterparty = cpty,
                instrument = instName,
                signedQuantity = (rng.nextInt(5) + 1) * 100L,
                price = price2,
                tradeDate = config.businessDate,
                settlementDate = settlementDate,
                positionKey = "$book#$cpty#$instName"
            ))

            // Trade 3: Sell partial - moving towards zero, WAC unchanged
            val price3 = varyPrice(basePrice, rng)
            trades.add(GeneratedTrade(
                sequenceNum = seq++,
                book = book,
                counterparty = cpty,
                instrument = instName,
                signedQuantity = -(rng.nextInt(2) + 1) * 100L,
                price = price3,
                tradeDate = config.businessDate,
                settlementDate = settlementDate,
                positionKey = "$book#$cpty#$instName"
            ))
        }

        // Phase 2: Fill remaining trades with random distribution
        val remaining = config.trades - trades.size
        for (i in 0 until remaining.coerceAtLeast(0)) {
            val book = books[i % books.size]
            val cpty = cptys[i % cptys.size]
            val instPair = instruments[i % instruments.size]
            val isBuy = rng.nextDouble() < 0.7
            val qty = (rng.nextInt(10) + 1) * 100L * if (isBuy) 1 else -1
            val price = varyPrice(instPair.second, rng)

            trades.add(GeneratedTrade(
                sequenceNum = seq++,
                book = book,
                counterparty = cpty,
                instrument = instPair.first,
                signedQuantity = qty,
                price = price,
                tradeDate = config.businessDate,
                settlementDate = settlementDate,
                positionKey = "$book#$cpty#${instPair.first}"
            ))
        }

        return trades
    }

    private fun varyPrice(base: BigDecimal, rng: Random): BigDecimal {
        // Vary by +/- 5%
        val factor = BigDecimal.ONE + BigDecimal(rng.nextDouble(-0.05, 0.05).toString())
        return (base * factor).setScale(6, RoundingMode.HALF_UP)
    }
}
