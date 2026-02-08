package com.positionservice.db

import com.positionservice.domain.PositionKey
import com.positionservice.domain.UpsertResult
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Instant
import java.time.LocalDate

class PositionKeyRepository {

    fun upsertPositionKey(
        positionKey: String,
        configId: Long,
        configType: String,
        configName: String,
        book: String?,
        counterparty: String?,
        instrument: String?,
        tradeDate: LocalDate,
        settlementDate: LocalDate,
        sequenceNum: Long
    ): UpsertResult = transaction {
        // Try to find existing
        val existing = PositionKeysTable
            .selectAll()
            .where {
                (PositionKeysTable.positionKey eq positionKey) and
                (PositionKeysTable.configId eq configId)
            }
            .singleOrNull()

        if (existing != null) {
            val positionId = existing[PositionKeysTable.positionId]
            val currentTradeDate = existing[PositionKeysTable.lastTradeDate]
            val currentSettlementDate = existing[PositionKeysTable.lastSettlementDate]

            // Update with GREATEST logic
            val newTradeDate = maxOf(currentTradeDate, tradeDate)
            val newSettlementDate = maxOf(currentSettlementDate, settlementDate)

            PositionKeysTable.update({
                (PositionKeysTable.positionKey eq positionKey) and
                (PositionKeysTable.configId eq configId)
            }) {
                it[lastTradeDate] = newTradeDate
                it[lastSettlementDate] = newSettlementDate
            }

            UpsertResult(
                positionId = positionId,
                lastTradeDate = currentTradeDate,
                lastSettlementDate = currentSettlementDate
            )
        } else {
            val id = PositionKeysTable.insert {
                it[PositionKeysTable.positionKey] = positionKey
                it[PositionKeysTable.configId] = configId
                it[PositionKeysTable.configType] = configType
                it[PositionKeysTable.configName] = configName
                it[PositionKeysTable.book] = book
                it[PositionKeysTable.counterparty] = counterparty
                it[PositionKeysTable.instrument] = instrument
                it[lastTradeDate] = tradeDate
                it[lastSettlementDate] = settlementDate
                it[createdAt] = Instant.now()
                it[createdBySequence] = sequenceNum
            } get PositionKeysTable.positionId

            UpsertResult(
                positionId = id,
                lastTradeDate = null,
                lastSettlementDate = null
            )
        }
    }

    fun search(
        book: String? = null,
        counterparty: String? = null,
        instrument: String? = null,
        limit: Int = 500
    ): List<PositionKey> = transaction {
        PositionKeysTable.selectAll().apply {
            book?.let { andWhere { PositionKeysTable.book eq it } }
            counterparty?.let { andWhere { PositionKeysTable.counterparty eq it } }
            instrument?.let { andWhere { PositionKeysTable.instrument eq it } }
        }
            .orderBy(PositionKeysTable.positionKey)
            .limit(limit)
            .map { rowToPositionKey(it) }
    }

    fun distinctBooks(): List<String> = transaction {
        PositionKeysTable.select(PositionKeysTable.book)
            .where { PositionKeysTable.book.isNotNull() }
            .withDistinct()
            .orderBy(PositionKeysTable.book)
            .map { it[PositionKeysTable.book]!! }
    }

    fun distinctCounterparties(): List<String> = transaction {
        PositionKeysTable.select(PositionKeysTable.counterparty)
            .where { PositionKeysTable.counterparty.isNotNull() }
            .withDistinct()
            .orderBy(PositionKeysTable.counterparty)
            .map { it[PositionKeysTable.counterparty]!! }
    }

    fun distinctInstruments(): List<String> = transaction {
        PositionKeysTable.select(PositionKeysTable.instrument)
            .where { PositionKeysTable.instrument.isNotNull() }
            .withDistinct()
            .orderBy(PositionKeysTable.instrument)
            .map { it[PositionKeysTable.instrument]!! }
    }

    private fun rowToPositionKey(row: ResultRow): PositionKey = PositionKey(
        positionId = row[PositionKeysTable.positionId],
        positionKey = row[PositionKeysTable.positionKey],
        configId = row[PositionKeysTable.configId],
        configType = row[PositionKeysTable.configType],
        configName = row[PositionKeysTable.configName],
        book = row[PositionKeysTable.book],
        counterparty = row[PositionKeysTable.counterparty],
        instrument = row[PositionKeysTable.instrument],
        lastTradeDate = row[PositionKeysTable.lastTradeDate],
        lastSettlementDate = row[PositionKeysTable.lastSettlementDate],
        createdAt = row[PositionKeysTable.createdAt],
        createdBySequence = row[PositionKeysTable.createdBySequence]
    )

    private fun maxOf(a: LocalDate?, b: LocalDate): LocalDate =
        if (a == null || b.isAfter(a)) b else a
}
