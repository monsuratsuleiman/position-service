package com.positionservice.e2e

import com.positionservice.calculation.PositionCalculationEngine
import com.positionservice.db.PositionKeyRepository
import com.positionservice.db.PositionTradeRepository
import com.positionservice.domain.*
import java.math.BigDecimal
import java.math.RoundingMode
import java.time.Instant
import java.time.LocalDate
import java.util.UUID

object TestDataSeeder {

    fun bd(value: String): BigDecimal = BigDecimal(value).setScale(12, RoundingMode.HALF_UP)

    fun createTradeEvent(
        seq: Long,
        book: String = "BOOK1",
        cpty: String = "GOLDMAN",
        instrument: String = "AAPL",
        qty: Long = 100,
        price: BigDecimal = bd("150.000000"),
        tradeDate: LocalDate = LocalDate.of(2025, 1, 20),
        settlementDate: LocalDate = LocalDate.of(2025, 1, 22),
        source: String = "BLOOMBERG",
        sourceId: String = "BLM-${seq}",
        tradeTime: Instant = Instant.parse("2025-01-20T10:00:00Z")
    ): TradeEvent = TradeEvent(
        sequenceNum = seq,
        book = book,
        counterparty = cpty,
        instrument = instrument,
        signedQuantity = qty,
        price = price,
        tradeTime = tradeTime,
        tradeDate = tradeDate,
        settlementDate = settlementDate,
        source = source,
        sourceId = sourceId
    )

    fun createCalcRequest(
        positionKey: String = "BOOK1#GOLDMAN#AAPL",
        businessDate: LocalDate = LocalDate.of(2025, 1, 20),
        dateBasis: DateBasis = DateBasis.TRADE_DATE,
        positionId: Long = 1L,
        triggeringTradeSequence: Long = 1L,
        changeReason: ChangeReason = ChangeReason.INITIAL,
        priceMethods: Set<PriceCalculationMethod> = setOf(PriceCalculationMethod.WAC),
        requestId: String = UUID.randomUUID().toString()
    ): PositionCalcRequest = PositionCalcRequest(
        requestId = requestId,
        positionId = positionId,
        positionKey = positionKey,
        dateBasis = dateBasis,
        businessDate = businessDate,
        priceMethods = priceMethods,
        triggeringTradeSequence = triggeringTradeSequence,
        changeReason = changeReason
    )

    fun insertTradeAndCalc(
        engine: PositionCalculationEngine,
        tradeRepo: PositionTradeRepository,
        positionKeyRepo: PositionKeyRepository,
        trade: TradeEvent
    ) {
        val config = PositionConfig.seed()
        val positionKey = config.keyFormat.generateKey(trade.book, trade.counterparty, trade.instrument)

        // Insert the trade
        tradeRepo.insertTrade(trade, positionKey)

        // Upsert position key
        val upsertResult = positionKeyRepo.upsertPositionKey(
            positionKey = positionKey,
            configId = config.configId,
            configType = config.type.name,
            configName = config.name,
            book = trade.book,
            counterparty = trade.counterparty,
            instrument = trade.instrument,
            tradeDate = trade.tradeDate,
            settlementDate = trade.settlementDate,
            sequenceNum = trade.sequenceNum
        )

        // Run calculation for trade date basis
        val tradeDateRequest = createCalcRequest(
            positionKey = positionKey,
            businessDate = trade.tradeDate,
            dateBasis = DateBasis.TRADE_DATE,
            positionId = upsertResult.positionId,
            triggeringTradeSequence = trade.sequenceNum
        )
        engine.calculatePosition(tradeDateRequest)

        // Run calculation for settlement date basis
        val settlementDateRequest = createCalcRequest(
            positionKey = positionKey,
            businessDate = trade.settlementDate,
            dateBasis = DateBasis.SETTLEMENT_DATE,
            positionId = upsertResult.positionId,
            triggeringTradeSequence = trade.sequenceNum
        )
        engine.calculatePosition(settlementDateRequest)
    }
}
