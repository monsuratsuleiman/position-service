package com.positionservice.db

import com.positionservice.domain.*
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

open class PositionSnapshotRepository {

    open fun findSnapshot(
        positionKey: String,
        businessDate: LocalDate,
        dateBasis: DateBasis
    ): PositionSnapshot? = transaction {
        when (dateBasis) {
            DateBasis.TRADE_DATE -> findSnapshotTrade(positionKey, businessDate)
            DateBasis.SETTLEMENT_DATE -> findSnapshotSettled(positionKey, businessDate)
        }
    }

    open fun saveSnapshot(
        snapshot: PositionSnapshot,
        dateBasis: DateBasis,
        changeReason: ChangeReason
    ): Unit = transaction {
        when (dateBasis) {
            DateBasis.TRADE_DATE -> saveSnapshotTrade(snapshot, changeReason)
            DateBasis.SETTLEMENT_DATE -> saveSnapshotSettled(snapshot, changeReason)
        }
    }

    open fun findSnapshotsForPosition(
        positionKey: String,
        dateBasis: DateBasis,
        fromDate: LocalDate? = null,
        toDate: LocalDate? = null
    ): List<PositionSnapshot> = transaction {
        when (dateBasis) {
            DateBasis.TRADE_DATE -> findSnapshotsForPositionTrade(positionKey, fromDate, toDate)
            DateBasis.SETTLEMENT_DATE -> findSnapshotsForPositionSettled(positionKey, fromDate, toDate)
        }
    }

    open fun findSnapshotHistory(
        positionKey: String,
        businessDate: LocalDate,
        dateBasis: DateBasis
    ): List<PositionSnapshotHistoryRecord> = transaction {
        when (dateBasis) {
            DateBasis.TRADE_DATE -> findHistoryTrade(positionKey, businessDate)
            DateBasis.SETTLEMENT_DATE -> findHistorySettled(positionKey, businessDate)
        }
    }

    // -- Trade Date implementations --

    private fun findSnapshotTrade(positionKey: String, businessDate: LocalDate): PositionSnapshot? {
        val t = PositionSnapshotsTable
        return t.selectAll()
            .where { (t.positionKey eq positionKey) and (t.businessDate eq businessDate) }
            .singleOrNull()
            ?.let { row ->
                PositionSnapshot(
                    positionKey = row[t.positionKey],
                    businessDate = row[t.businessDate],
                    netQuantity = row[t.netQuantity],
                    grossLong = row[t.grossLong],
                    grossShort = row[t.grossShort],
                    tradeCount = row[t.tradeCount],
                    totalNotional = row[t.totalNotional] ?: BigDecimal.ZERO,
                    calculationVersion = row[t.calculationVersion],
                    calculatedAt = row[t.calculatedAt],
                    calculationMethod = parseCalcMethod(row[t.calculationMethod]),
                    calculationRequestId = row[t.calculationRequestId],
                    lastSequenceNum = row[t.lastSequenceNum],
                    lastTradeTime = row[t.lastTradeTime]
                )
            }
    }

    private fun findSnapshotSettled(positionKey: String, businessDate: LocalDate): PositionSnapshot? {
        val t = PositionSnapshotsSettledTable
        return t.selectAll()
            .where { (t.positionKey eq positionKey) and (t.businessDate eq businessDate) }
            .singleOrNull()
            ?.let { row ->
                PositionSnapshot(
                    positionKey = row[t.positionKey],
                    businessDate = row[t.businessDate],
                    netQuantity = row[t.netQuantity],
                    grossLong = row[t.grossLong],
                    grossShort = row[t.grossShort],
                    tradeCount = row[t.tradeCount],
                    totalNotional = row[t.totalNotional] ?: BigDecimal.ZERO,
                    calculationVersion = row[t.calculationVersion],
                    calculatedAt = row[t.calculatedAt],
                    calculationMethod = parseCalcMethod(row[t.calculationMethod]),
                    calculationRequestId = row[t.calculationRequestId],
                    lastSequenceNum = row[t.lastSequenceNum],
                    lastTradeTime = row[t.lastTradeTime]
                )
            }
    }

    private fun saveSnapshotTrade(snapshot: PositionSnapshot, changeReason: ChangeReason) {
        val t = PositionSnapshotsTable
        val h = PositionSnapshotsHistoryTable

        val existing = t.selectAll()
            .where { (t.positionKey eq snapshot.positionKey) and (t.businessDate eq snapshot.businessDate) }
            .singleOrNull()

        if (existing != null) {
            val prevVersion = existing[t.calculationVersion]
            val prevNetQty = existing[t.netQuantity]

            h.update({
                (h.positionKey eq snapshot.positionKey) and
                (h.businessDate eq snapshot.businessDate) and
                (h.supersededAt.isNull())
            }) { it[h.supersededAt] = Instant.now() }

            t.update({
                (t.positionKey eq snapshot.positionKey) and (t.businessDate eq snapshot.businessDate)
            }) {
                it[t.netQuantity] = snapshot.netQuantity
                it[t.grossLong] = snapshot.grossLong
                it[t.grossShort] = snapshot.grossShort
                it[t.tradeCount] = snapshot.tradeCount
                it[t.totalNotional] = snapshot.totalNotional
                it[t.calculationVersion] = prevVersion + 1
                it[t.calculatedAt] = Instant.now()
                it[t.calculationMethod] = snapshot.calculationMethod.name
                it[t.calculationRequestId] = snapshot.calculationRequestId
                it[t.lastSequenceNum] = snapshot.lastSequenceNum
                it[t.lastTradeTime] = snapshot.lastTradeTime
            }

            insertHistoryTrade(h, snapshot, prevVersion + 1, changeReason, prevNetQty)
        } else {
            t.insert {
                it[t.positionKey] = snapshot.positionKey
                it[t.businessDate] = snapshot.businessDate
                it[t.netQuantity] = snapshot.netQuantity
                it[t.grossLong] = snapshot.grossLong
                it[t.grossShort] = snapshot.grossShort
                it[t.tradeCount] = snapshot.tradeCount
                it[t.totalNotional] = snapshot.totalNotional
                it[t.calculationVersion] = 1
                it[t.calculatedAt] = Instant.now()
                it[t.calculationMethod] = snapshot.calculationMethod.name
                it[t.calculationRequestId] = snapshot.calculationRequestId
                it[t.lastSequenceNum] = snapshot.lastSequenceNum
                it[t.lastTradeTime] = snapshot.lastTradeTime
            }
            insertHistoryTrade(h, snapshot, 1, changeReason, null)
        }
    }

    private fun saveSnapshotSettled(snapshot: PositionSnapshot, changeReason: ChangeReason) {
        val t = PositionSnapshotsSettledTable
        val h = PositionSnapshotsSettledHistoryTable

        val existing = t.selectAll()
            .where { (t.positionKey eq snapshot.positionKey) and (t.businessDate eq snapshot.businessDate) }
            .singleOrNull()

        if (existing != null) {
            val prevVersion = existing[t.calculationVersion]
            val prevNetQty = existing[t.netQuantity]

            h.update({
                (h.positionKey eq snapshot.positionKey) and
                (h.businessDate eq snapshot.businessDate) and
                (h.supersededAt.isNull())
            }) { it[h.supersededAt] = Instant.now() }

            t.update({
                (t.positionKey eq snapshot.positionKey) and (t.businessDate eq snapshot.businessDate)
            }) {
                it[t.netQuantity] = snapshot.netQuantity
                it[t.grossLong] = snapshot.grossLong
                it[t.grossShort] = snapshot.grossShort
                it[t.tradeCount] = snapshot.tradeCount
                it[t.totalNotional] = snapshot.totalNotional
                it[t.calculationVersion] = prevVersion + 1
                it[t.calculatedAt] = Instant.now()
                it[t.calculationMethod] = snapshot.calculationMethod.name
                it[t.calculationRequestId] = snapshot.calculationRequestId
                it[t.lastSequenceNum] = snapshot.lastSequenceNum
                it[t.lastTradeTime] = snapshot.lastTradeTime
            }

            insertHistorySettled(h, snapshot, prevVersion + 1, changeReason, prevNetQty)
        } else {
            t.insert {
                it[t.positionKey] = snapshot.positionKey
                it[t.businessDate] = snapshot.businessDate
                it[t.netQuantity] = snapshot.netQuantity
                it[t.grossLong] = snapshot.grossLong
                it[t.grossShort] = snapshot.grossShort
                it[t.tradeCount] = snapshot.tradeCount
                it[t.totalNotional] = snapshot.totalNotional
                it[t.calculationVersion] = 1
                it[t.calculatedAt] = Instant.now()
                it[t.calculationMethod] = snapshot.calculationMethod.name
                it[t.calculationRequestId] = snapshot.calculationRequestId
                it[t.lastSequenceNum] = snapshot.lastSequenceNum
                it[t.lastTradeTime] = snapshot.lastTradeTime
            }
            insertHistorySettled(h, snapshot, 1, changeReason, null)
        }
    }

    private fun findSnapshotsForPositionTrade(
        positionKey: String, fromDate: LocalDate?, toDate: LocalDate?
    ): List<PositionSnapshot> {
        val t = PositionSnapshotsTable
        return t.selectAll()
            .where {
                var cond: Op<Boolean> = t.positionKey eq positionKey
                if (fromDate != null) cond = cond and (t.businessDate greaterEq fromDate)
                if (toDate != null) cond = cond and (t.businessDate lessEq toDate)
                cond
            }
            .orderBy(t.businessDate, SortOrder.ASC)
            .map { row ->
                PositionSnapshot(
                    positionKey = row[t.positionKey],
                    businessDate = row[t.businessDate],
                    netQuantity = row[t.netQuantity],
                    grossLong = row[t.grossLong],
                    grossShort = row[t.grossShort],
                    tradeCount = row[t.tradeCount],
                    totalNotional = row[t.totalNotional] ?: BigDecimal.ZERO,
                    calculationVersion = row[t.calculationVersion],
                    calculatedAt = row[t.calculatedAt],
                    calculationMethod = parseCalcMethod(row[t.calculationMethod]),
                    calculationRequestId = row[t.calculationRequestId],
                    lastSequenceNum = row[t.lastSequenceNum],
                    lastTradeTime = row[t.lastTradeTime]
                )
            }
    }

    private fun findSnapshotsForPositionSettled(
        positionKey: String, fromDate: LocalDate?, toDate: LocalDate?
    ): List<PositionSnapshot> {
        val t = PositionSnapshotsSettledTable
        return t.selectAll()
            .where {
                var cond: Op<Boolean> = t.positionKey eq positionKey
                if (fromDate != null) cond = cond and (t.businessDate greaterEq fromDate)
                if (toDate != null) cond = cond and (t.businessDate lessEq toDate)
                cond
            }
            .orderBy(t.businessDate, SortOrder.ASC)
            .map { row ->
                PositionSnapshot(
                    positionKey = row[t.positionKey],
                    businessDate = row[t.businessDate],
                    netQuantity = row[t.netQuantity],
                    grossLong = row[t.grossLong],
                    grossShort = row[t.grossShort],
                    tradeCount = row[t.tradeCount],
                    totalNotional = row[t.totalNotional] ?: BigDecimal.ZERO,
                    calculationVersion = row[t.calculationVersion],
                    calculatedAt = row[t.calculatedAt],
                    calculationMethod = parseCalcMethod(row[t.calculationMethod]),
                    calculationRequestId = row[t.calculationRequestId],
                    lastSequenceNum = row[t.lastSequenceNum],
                    lastTradeTime = row[t.lastTradeTime]
                )
            }
    }

    private fun findHistoryTrade(positionKey: String, businessDate: LocalDate): List<PositionSnapshotHistoryRecord> {
        val h = PositionSnapshotsHistoryTable
        return h.selectAll()
            .where { (h.positionKey eq positionKey) and (h.businessDate eq businessDate) }
            .orderBy(h.calculationVersion, SortOrder.ASC)
            .map { row -> rowToHistory(row, h.historyId, h.positionKey, h.businessDate, h.netQuantity, h.grossLong, h.grossShort, h.tradeCount, h.totalNotional, h.calculationVersion, h.calculatedAt, h.supersededAt, h.changeReason, h.previousNetQuantity, h.calculationMethod) }
    }

    private fun findHistorySettled(positionKey: String, businessDate: LocalDate): List<PositionSnapshotHistoryRecord> {
        val h = PositionSnapshotsSettledHistoryTable
        return h.selectAll()
            .where { (h.positionKey eq positionKey) and (h.businessDate eq businessDate) }
            .orderBy(h.calculationVersion, SortOrder.ASC)
            .map { row -> rowToHistory(row, h.historyId, h.positionKey, h.businessDate, h.netQuantity, h.grossLong, h.grossShort, h.tradeCount, h.totalNotional, h.calculationVersion, h.calculatedAt, h.supersededAt, h.changeReason, h.previousNetQuantity, h.calculationMethod) }
    }

    private fun rowToHistory(
        row: ResultRow,
        historyIdCol: Column<Long>,
        positionKeyCol: Column<String>,
        businessDateCol: Column<LocalDate>,
        netQuantityCol: Column<Long>,
        grossLongCol: Column<Long>,
        grossShortCol: Column<Long>,
        tradeCountCol: Column<Int>,
        totalNotionalCol: Column<BigDecimal?>,
        calculationVersionCol: Column<Int>,
        calculatedAtCol: Column<java.time.Instant>,
        supersededAtCol: Column<java.time.Instant?>,
        changeReasonCol: Column<String?>,
        previousNetQuantityCol: Column<Long?>,
        calculationMethodCol: Column<String?>
    ) = PositionSnapshotHistoryRecord(
        historyId = row[historyIdCol],
        positionKey = row[positionKeyCol],
        businessDate = row[businessDateCol],
        netQuantity = row[netQuantityCol],
        grossLong = row[grossLongCol],
        grossShort = row[grossShortCol],
        tradeCount = row[tradeCountCol],
        totalNotional = row[totalNotionalCol] ?: BigDecimal.ZERO,
        calculationVersion = row[calculationVersionCol],
        calculatedAt = row[calculatedAtCol],
        supersededAt = row[supersededAtCol],
        changeReason = row[changeReasonCol],
        previousNetQuantity = row[previousNetQuantityCol],
        calculationMethod = row[calculationMethodCol]
    )

    private fun insertHistoryTrade(
        h: PositionSnapshotsHistoryTable,
        snapshot: PositionSnapshot,
        version: Int,
        changeReason: ChangeReason,
        previousNetQty: Long?
    ) {
        h.insert {
            it[h.positionKey] = snapshot.positionKey
            it[h.businessDate] = snapshot.businessDate
            it[h.netQuantity] = snapshot.netQuantity
            it[h.grossLong] = snapshot.grossLong
            it[h.grossShort] = snapshot.grossShort
            it[h.tradeCount] = snapshot.tradeCount
            it[h.totalNotional] = snapshot.totalNotional
            it[h.calculationVersion] = version
            it[h.calculatedAt] = Instant.now()
            it[h.supersededAt] = null
            it[h.changeReason] = changeReason.name
            it[h.previousNetQuantity] = previousNetQty
            it[h.calculationRequestId] = snapshot.calculationRequestId
            it[h.lastSequenceNum] = snapshot.lastSequenceNum
            it[h.lastTradeTime] = snapshot.lastTradeTime
            it[h.calculationMethod] = snapshot.calculationMethod.name
        }
    }

    private fun insertHistorySettled(
        h: PositionSnapshotsSettledHistoryTable,
        snapshot: PositionSnapshot,
        version: Int,
        changeReason: ChangeReason,
        previousNetQty: Long?
    ) {
        h.insert {
            it[h.positionKey] = snapshot.positionKey
            it[h.businessDate] = snapshot.businessDate
            it[h.netQuantity] = snapshot.netQuantity
            it[h.grossLong] = snapshot.grossLong
            it[h.grossShort] = snapshot.grossShort
            it[h.tradeCount] = snapshot.tradeCount
            it[h.totalNotional] = snapshot.totalNotional
            it[h.calculationVersion] = version
            it[h.calculatedAt] = Instant.now()
            it[h.supersededAt] = null
            it[h.changeReason] = changeReason.name
            it[h.previousNetQuantity] = previousNetQty
            it[h.calculationRequestId] = snapshot.calculationRequestId
            it[h.lastSequenceNum] = snapshot.lastSequenceNum
            it[h.lastTradeTime] = snapshot.lastTradeTime
            it[h.calculationMethod] = snapshot.calculationMethod.name
        }
    }

    private fun parseCalcMethod(value: String?): CalculationMethod = try {
        CalculationMethod.valueOf(value ?: "FULL_RECALC")
    } catch (_: Exception) {
        CalculationMethod.FULL_RECALC
    }
}

data class PositionSnapshotHistoryRecord(
    val historyId: Long,
    val positionKey: String,
    val businessDate: LocalDate,
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val totalNotional: BigDecimal,
    val calculationVersion: Int,
    val calculatedAt: java.time.Instant,
    val supersededAt: java.time.Instant?,
    val changeReason: String?,
    val previousNetQuantity: Long?,
    val calculationMethod: String?
)
