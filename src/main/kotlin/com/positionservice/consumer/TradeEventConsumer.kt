package com.positionservice.consumer

import com.positionservice.db.PositionConfigRepository
import com.positionservice.db.PositionKeyRepository
import com.positionservice.db.PositionTradeRepository
import com.positionservice.domain.*
import com.positionservice.kafka.KafkaProducerWrapper
import org.slf4j.LoggerFactory
import java.time.LocalDate
import java.util.UUID

class TradeEventConsumer(
    private val tradeRepo: PositionTradeRepository,
    private val positionKeyRepo: PositionKeyRepository,
    private val kafkaProducer: KafkaProducerWrapper,
    private val configRepo: PositionConfigRepository
) {

    private val logger = LoggerFactory.getLogger(TradeEventConsumer::class.java)

    private data class CalcIntentKey(
        val positionKey: String,
        val dateBasis: DateBasis,
        val businessDate: LocalDate
    )

    private data class CalcIntent(
        val positionId: Long,
        val positionKey: String,
        val dateBasis: DateBasis,
        val businessDate: LocalDate,
        val sequenceNum: Long,
        val changeReason: ChangeReason,
        val config: PositionConfig
    )

    @Volatile
    private var cachedConfigs: List<PositionConfig> = emptyList()
    private var lastConfigRefresh: Long = 0L
    private val configRefreshIntervalMs = 60_000L

    private fun getActiveConfigs(): List<PositionConfig> {
        val now = System.currentTimeMillis()
        if (cachedConfigs.isEmpty() || now - lastConfigRefresh > configRefreshIntervalMs) {
            cachedConfigs = configRepo.findActive()
            lastConfigRefresh = now
            logger.debug("Refreshed active configs: {} configs loaded", cachedConfigs.size)
        }
        return cachedConfigs
    }

    fun processTradesBatch(trades: List<TradeEvent>) {
        if (trades.isEmpty()) return
        logger.debug("Processing batch of {} trades", trades.size)

        // 1. Build (trade, canonicalKey) pairs and batch-insert in single transaction
        val tradesWithKeys = trades.map { trade ->
            val canonicalKey = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT.generateKey(
                trade.book, trade.counterparty, trade.instrument
            )
            trade to canonicalKey
        }
        val inserted = tradeRepo.batchInsertTrades(tradesWithKeys)
        if (inserted.isEmpty()) {
            logger.info("Batch of {} trades all duplicates, skipping", trades.size)
            return
        }
        logger.debug("Inserted {} of {} trades", inserted.size, trades.size)

        // 2. For each inserted trade: upsert position key, collect calc intents
        val calcIntents = mutableMapOf<CalcIntentKey, CalcIntent>()
        val allConfigs = getActiveConfigs()

        for ((trade, _) in inserted) {
            val configs = allConfigs.filter { it.scope.matches(trade) }
            for (config in configs) {
                val positionKey = config.keyFormat.generateKey(trade.book, trade.counterparty, trade.instrument)
                val (book, counterparty, instrument) = config.keyFormat.extractDimensions(
                    trade.book, trade.counterparty, trade.instrument
                )

                val upsertResult = positionKeyRepo.upsertPositionKey(
                    positionKey = positionKey,
                    configId = config.configId,
                    configType = config.type.name,
                    configName = config.name,
                    book = book,
                    counterparty = counterparty,
                    instrument = instrument,
                    tradeDate = trade.tradeDate,
                    settlementDate = trade.settlementDate,
                    sequenceNum = trade.sequenceNum
                )

                // Collect trade-date intents
                collectCalcIntents(
                    calcIntents, upsertResult.positionId, positionKey, DateBasis.TRADE_DATE,
                    trade.tradeDate, upsertResult.lastTradeDate, trade.sequenceNum, config
                )

                // Collect settlement-date intents
                collectCalcIntents(
                    calcIntents, upsertResult.positionId, positionKey, DateBasis.SETTLEMENT_DATE,
                    trade.settlementDate, upsertResult.lastSettlementDate, trade.sequenceNum, config
                )
            }
        }

        // 3. Publish deduped calc requests
        logger.debug("Publishing {} deduped calc requests (from {} inserted trades)", calcIntents.size, inserted.size)
        for ((_, intent) in calcIntents) {
            publishSingleRequest(
                intent.positionId, intent.positionKey, intent.dateBasis,
                intent.businessDate, intent.sequenceNum, intent.changeReason, intent.config
            )
        }
    }

    private fun collectCalcIntents(
        intents: MutableMap<CalcIntentKey, CalcIntent>,
        positionId: Long,
        positionKey: String,
        dateBasis: DateBasis,
        tradeDate: LocalDate,
        lastDate: LocalDate?,
        sequenceNum: Long,
        config: PositionConfig
    ) {
        if (lastDate != null && tradeDate.isBefore(lastDate)) {
            // Late trade: cascade from tradeDate to lastDate
            var date = tradeDate
            while (!date.isAfter(lastDate)) {
                addIntent(intents, positionId, positionKey, dateBasis, date, sequenceNum, ChangeReason.LATE_TRADE, config)
                date = date.plusDays(1)
            }
        } else {
            addIntent(intents, positionId, positionKey, dateBasis, tradeDate, sequenceNum, ChangeReason.INITIAL, config)
        }
    }

    private fun addIntent(
        intents: MutableMap<CalcIntentKey, CalcIntent>,
        positionId: Long,
        positionKey: String,
        dateBasis: DateBasis,
        businessDate: LocalDate,
        sequenceNum: Long,
        changeReason: ChangeReason,
        config: PositionConfig
    ) {
        val key = CalcIntentKey(positionKey, dateBasis, businessDate)
        val existing = intents[key]
        if (existing == null) {
            intents[key] = CalcIntent(positionId, positionKey, dateBasis, businessDate, sequenceNum, changeReason, config)
        } else {
            // Keep highest sequenceNum; promote to LATE_TRADE if any intent is LATE_TRADE
            val mergedReason = if (existing.changeReason == ChangeReason.LATE_TRADE || changeReason == ChangeReason.LATE_TRADE) {
                ChangeReason.LATE_TRADE
            } else {
                existing.changeReason
            }
            val mergedSeq = maxOf(existing.sequenceNum, sequenceNum)
            intents[key] = existing.copy(sequenceNum = mergedSeq, changeReason = mergedReason)
        }
    }

    fun processTrade(trade: TradeEvent) {
        logger.debug("Processing trade seq={} {} {} @ {}", trade.sequenceNum, trade.instrument, trade.signedQuantity, trade.price)

        // 1. Store trade once using canonical BOOK_COUNTERPARTY_INSTRUMENT key
        val canonicalKey = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT.generateKey(trade.book, trade.counterparty, trade.instrument)
        val inserted = tradeRepo.insertTrade(trade, canonicalKey)
        if (!inserted) {
            logger.info("Duplicate trade ignored: seq={}", trade.sequenceNum)
            return
        }

        // 2. For each active config, generate key, upsert position_keys, and publish calc requests
        val allConfigs = getActiveConfigs()
        val configs = allConfigs.filter { it.scope.matches(trade) }
        if (configs.size < allConfigs.size) {
            logger.debug("Scope filtering: {} of {} configs match trade seq={}",
                configs.size, allConfigs.size, trade.sequenceNum)
        }
        for (config in configs) {
            val positionKey = config.keyFormat.generateKey(trade.book, trade.counterparty, trade.instrument)

            // Extract only dimensions relevant to this key format
            val (book, counterparty, instrument) = config.keyFormat.extractDimensions(
                trade.book, trade.counterparty, trade.instrument
            )

            // Upsert position_keys (get ID + update last dates)
            val upsertResult = positionKeyRepo.upsertPositionKey(
                positionKey = positionKey,
                configId = config.configId,
                configType = config.type.name,
                configName = config.name,
                book = book,
                counterparty = counterparty,
                instrument = instrument,
                tradeDate = trade.tradeDate,
                settlementDate = trade.settlementDate,
                sequenceNum = trade.sequenceNum
            )

            // Publish calculation requests for trade date
            publishCalcRequests(
                positionId = upsertResult.positionId,
                positionKey = positionKey,
                dateBasis = DateBasis.TRADE_DATE,
                tradeDate = trade.tradeDate,
                lastDate = upsertResult.lastTradeDate,
                sequenceNum = trade.sequenceNum,
                config = config
            )

            // Publish calculation requests for settlement date
            publishCalcRequests(
                positionId = upsertResult.positionId,
                positionKey = positionKey,
                dateBasis = DateBasis.SETTLEMENT_DATE,
                tradeDate = trade.settlementDate,
                lastDate = upsertResult.lastSettlementDate,
                sequenceNum = trade.sequenceNum,
                config = config
            )
        }
    }

    private fun publishCalcRequests(
        positionId: Long,
        positionKey: String,
        dateBasis: DateBasis,
        tradeDate: LocalDate,
        lastDate: LocalDate?,
        sequenceNum: Long,
        config: PositionConfig
    ) {
        if (lastDate != null && tradeDate.isBefore(lastDate)) {
            // Late trade: cascade from tradeDate to lastDate
            logger.info(
                "Late trade cascade for {} {}: {} -> {} ({} dates)",
                positionKey, dateBasis, tradeDate, lastDate,
                tradeDate.datesUntil(lastDate.plusDays(1)).count()
            )

            var date = tradeDate
            while (!date.isAfter(lastDate)) {
                publishSingleRequest(positionId, positionKey, dateBasis, date, sequenceNum, ChangeReason.LATE_TRADE, config)
                date = date.plusDays(1)
            }
        } else {
            // Normal: single request for this date
            publishSingleRequest(positionId, positionKey, dateBasis, tradeDate, sequenceNum, ChangeReason.INITIAL, config)
        }
    }

    private fun publishSingleRequest(
        positionId: Long,
        positionKey: String,
        dateBasis: DateBasis,
        businessDate: LocalDate,
        sequenceNum: Long,
        changeReason: ChangeReason,
        config: PositionConfig
    ) {
        val request = PositionCalcRequest(
            requestId = UUID.randomUUID().toString(),
            positionId = positionId,
            positionKey = positionKey,
            dateBasis = dateBasis,
            businessDate = businessDate,
            priceMethods = config.priceMethods,
            triggeringTradeSequence = sequenceNum,
            changeReason = changeReason,
            keyFormat = config.keyFormat
        )
        kafkaProducer.publishCalcRequest(request)
    }
}
