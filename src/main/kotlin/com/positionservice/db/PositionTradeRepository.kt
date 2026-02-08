package com.positionservice.db

import com.positionservice.domain.DateBasis
import com.positionservice.domain.TradeEvent
import com.positionservice.domain.TradeMetrics
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.SqlExpressionBuilder.eq
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

open class PositionTradeRepository {

    open fun batchInsertTrades(trades: List<Pair<TradeEvent, String>>): List<Pair<TradeEvent, String>> = transaction {
        trades.filter { (trade, positionKey) ->
            val existing = PositionTradesTable.selectAll()
                .where { PositionTradesTable.sequenceNum eq trade.sequenceNum }
                .singleOrNull()
            if (existing != null) {
                false
            } else {
                PositionTradesTable.insert {
                    it[sequenceNum] = trade.sequenceNum
                    it[PositionTradesTable.positionKey] = positionKey
                    it[tradeTime] = trade.tradeTime
                    it[tradeDate] = trade.tradeDate
                    it[settlementDate] = trade.settlementDate
                    it[book] = trade.book
                    it[counterparty] = trade.counterparty
                    it[instrument] = trade.instrument
                    it[signedQuantity] = trade.signedQuantity
                    it[price] = trade.price
                    it[tradeSource] = trade.source
                    it[sourceId] = trade.sourceId
                    it[processedAt] = Instant.now()
                }
                true
            }
        }
    }

    open fun insertTrade(trade: TradeEvent, positionKey: String): Boolean = transaction {
        val existing = PositionTradesTable.selectAll()
            .where { PositionTradesTable.sequenceNum eq trade.sequenceNum }
            .singleOrNull()

        if (existing != null) return@transaction false // Idempotent

        PositionTradesTable.insert {
            it[sequenceNum] = trade.sequenceNum
            it[PositionTradesTable.positionKey] = positionKey
            it[tradeTime] = trade.tradeTime
            it[tradeDate] = trade.tradeDate
            it[settlementDate] = trade.settlementDate
            it[book] = trade.book
            it[counterparty] = trade.counterparty
            it[instrument] = trade.instrument
            it[signedQuantity] = trade.signedQuantity
            it[price] = trade.price
            it[tradeSource] = trade.source
            it[sourceId] = trade.sourceId
            it[processedAt] = Instant.now()
        }
        true
    }

    open fun aggregateMetrics(
        positionKey: String,
        businessDate: LocalDate,
        dateBasis: DateBasis
    ): TradeMetrics? = transaction {
        val dateColumn = when (dateBasis) {
            DateBasis.TRADE_DATE -> PositionTradesTable.tradeDate
            DateBasis.SETTLEMENT_DATE -> PositionTradesTable.settlementDate
        }

        val netQty = PositionTradesTable.signedQuantity.sum()
        val grossLongExpr = object : ExpressionWithColumnType<Long>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) {
                queryBuilder.append("SUM(CASE WHEN ")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(" > 0 THEN ")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(" ELSE 0 END)")
            }
            override val columnType = LongColumnType()
        }
        val grossShortExpr = object : ExpressionWithColumnType<Long>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) {
                queryBuilder.append("SUM(CASE WHEN ")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(" < 0 THEN ABS(")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(") ELSE 0 END)")
            }
            override val columnType = LongColumnType()
        }
        val tradeCountExpr = PositionTradesTable.sequenceNum.count()
        val totalNotionalExpr = object : ExpressionWithColumnType<BigDecimal>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) {
                queryBuilder.append("SUM(ABS(")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(") * ")
                queryBuilder.append(PositionTradesTable.price)
                queryBuilder.append(")")
            }
            override val columnType = DecimalColumnType(20, 6)
        }
        val maxSeq = PositionTradesTable.sequenceNum.max()
        val maxTradeTime = PositionTradesTable.tradeTime.max()

        val row = PositionTradesTable
            .select(netQty, grossLongExpr, grossShortExpr, tradeCountExpr, totalNotionalExpr, maxSeq, maxTradeTime)
            .where {
                (PositionTradesTable.positionKey eq positionKey) and (dateColumn eq businessDate)
            }
            .singleOrNull() ?: return@transaction null

        val count = row[tradeCountExpr]
        if (count == 0L) return@transaction null

        TradeMetrics(
            netQuantity = row[netQty] ?: 0L,
            grossLong = row[grossLongExpr] ?: 0L,
            grossShort = row[grossShortExpr] ?: 0L,
            tradeCount = count.toInt(),
            totalNotional = row[totalNotionalExpr] ?: BigDecimal.ZERO,
            lastSequenceNum = row[maxSeq] ?: 0L,
            lastTradeTime = row[maxTradeTime]
        )
    }

    open fun aggregateMetricsByDimensions(
        dimensions: Map<String, String>,
        businessDate: LocalDate,
        dateBasis: DateBasis
    ): TradeMetrics? = transaction {
        val dateColumn = when (dateBasis) {
            DateBasis.TRADE_DATE -> PositionTradesTable.tradeDate
            DateBasis.SETTLEMENT_DATE -> PositionTradesTable.settlementDate
        }

        val netQty = PositionTradesTable.signedQuantity.sum()
        val grossLongExpr = object : ExpressionWithColumnType<Long>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) {
                queryBuilder.append("SUM(CASE WHEN ")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(" > 0 THEN ")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(" ELSE 0 END)")
            }
            override val columnType = LongColumnType()
        }
        val grossShortExpr = object : ExpressionWithColumnType<Long>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) {
                queryBuilder.append("SUM(CASE WHEN ")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(" < 0 THEN ABS(")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(") ELSE 0 END)")
            }
            override val columnType = LongColumnType()
        }
        val tradeCountExpr = PositionTradesTable.sequenceNum.count()
        val totalNotionalExpr = object : ExpressionWithColumnType<BigDecimal>() {
            override fun toQueryBuilder(queryBuilder: QueryBuilder) {
                queryBuilder.append("SUM(ABS(")
                queryBuilder.append(PositionTradesTable.signedQuantity)
                queryBuilder.append(") * ")
                queryBuilder.append(PositionTradesTable.price)
                queryBuilder.append(")")
            }
            override val columnType = DecimalColumnType(20, 6)
        }
        val maxSeq = PositionTradesTable.sequenceNum.max()
        val maxTradeTime = PositionTradesTable.tradeTime.max()

        val row = PositionTradesTable
            .select(netQty, grossLongExpr, grossShortExpr, tradeCountExpr, totalNotionalExpr, maxSeq, maxTradeTime)
            .where {
                var cond: Op<Boolean> = dateColumn eq businessDate
                dimensions.forEach { (dim, value) ->
                    cond = cond and (dimensionColumn(dim) eq value)
                }
                cond
            }
            .singleOrNull() ?: return@transaction null

        val count = row[tradeCountExpr]
        if (count == 0L) return@transaction null

        TradeMetrics(
            netQuantity = row[netQty] ?: 0L,
            grossLong = row[grossLongExpr] ?: 0L,
            grossShort = row[grossShortExpr] ?: 0L,
            tradeCount = count.toInt(),
            totalNotional = row[totalNotionalExpr] ?: BigDecimal.ZERO,
            lastSequenceNum = row[maxSeq] ?: 0L,
            lastTradeTime = row[maxTradeTime]
        )
    }

    open fun findTradesByDimensions(
        dimensions: Map<String, String>,
        businessDate: LocalDate,
        dateBasis: DateBasis
    ): List<TradeRecord> = transaction {
        val dateColumn = when (dateBasis) {
            DateBasis.TRADE_DATE -> PositionTradesTable.tradeDate
            DateBasis.SETTLEMENT_DATE -> PositionTradesTable.settlementDate
        }

        PositionTradesTable
            .selectAll()
            .where {
                var cond: Op<Boolean> = dateColumn eq businessDate
                dimensions.forEach { (dim, value) ->
                    cond = cond and (dimensionColumn(dim) eq value)
                }
                cond
            }
            .orderBy(PositionTradesTable.sequenceNum, SortOrder.ASC)
            .map { row ->
                TradeRecord(
                    sequenceNum = row[PositionTradesTable.sequenceNum],
                    signedQuantity = row[PositionTradesTable.signedQuantity],
                    price = row[PositionTradesTable.price]
                )
            }
    }

    private fun dimensionColumn(dim: String): Column<String> = when (dim) {
        "book" -> PositionTradesTable.book
        "counterparty" -> PositionTradesTable.counterparty
        "instrument" -> PositionTradesTable.instrument
        else -> throw IllegalArgumentException("Unknown dimension: $dim")
    }

    open fun findTradesByPositionKeyAndDate(
        positionKey: String,
        businessDate: LocalDate,
        dateBasis: DateBasis
    ): List<TradeRecord> = transaction {
        val dateColumn = when (dateBasis) {
            DateBasis.TRADE_DATE -> PositionTradesTable.tradeDate
            DateBasis.SETTLEMENT_DATE -> PositionTradesTable.settlementDate
        }

        PositionTradesTable
            .selectAll()
            .where {
                (PositionTradesTable.positionKey eq positionKey) and (dateColumn eq businessDate)
            }
            .orderBy(PositionTradesTable.sequenceNum, SortOrder.ASC)
            .map { row ->
                TradeRecord(
                    sequenceNum = row[PositionTradesTable.sequenceNum],
                    signedQuantity = row[PositionTradesTable.signedQuantity],
                    price = row[PositionTradesTable.price]
                )
            }
    }

    open fun findTradesAfterSequence(
        positionKey: String,
        businessDate: LocalDate,
        dateBasis: DateBasis,
        afterSequence: Long
    ): List<TradeRecord> = transaction {
        val dateColumn = when (dateBasis) {
            DateBasis.TRADE_DATE -> PositionTradesTable.tradeDate
            DateBasis.SETTLEMENT_DATE -> PositionTradesTable.settlementDate
        }

        PositionTradesTable
            .selectAll()
            .where {
                (PositionTradesTable.positionKey eq positionKey) and
                (dateColumn eq businessDate) and
                (PositionTradesTable.sequenceNum greater afterSequence)
            }
            .orderBy(PositionTradesTable.sequenceNum, SortOrder.ASC)
            .map { row ->
                TradeRecord(
                    sequenceNum = row[PositionTradesTable.sequenceNum],
                    signedQuantity = row[PositionTradesTable.signedQuantity],
                    price = row[PositionTradesTable.price]
                )
            }
    }
}

data class TradeRecord(
    val sequenceNum: Long,
    val signedQuantity: Long,
    val price: BigDecimal
)
