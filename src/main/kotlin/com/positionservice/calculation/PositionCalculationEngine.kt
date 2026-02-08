package com.positionservice.calculation

import com.positionservice.db.PositionAveragePriceRepository
import com.positionservice.db.PositionSnapshotRepository
import com.positionservice.db.PositionTradeRepository
import com.positionservice.db.TradeRecord
import com.positionservice.domain.*
import org.slf4j.LoggerFactory
import java.math.BigDecimal
import java.time.Instant

class PositionCalculationEngine(
    private val tradeRepo: PositionTradeRepository,
    private val snapshotRepo: PositionSnapshotRepository,
    private val priceRepo: PositionAveragePriceRepository
) {

    private val logger = LoggerFactory.getLogger(PositionCalculationEngine::class.java)

    fun calculatePosition(request: PositionCalcRequest) {
        logger.debug("Calculating position for {} date={} basis={}", request.positionKey, request.businessDate, request.dateBasis)

        // Same-day incremental: if snapshot already exists for today and this is a new trade
        // (not a late-trade cascade which needs to recalc from updated prior-day state)
        if (request.changeReason == ChangeReason.INITIAL) {
            val existingToday = snapshotRepo.findSnapshot(request.positionKey, request.businessDate, request.dateBasis)
            if (existingToday != null) {
                calculateSameDayIncremental(request, existingToday)
                return
            }
        }

        // Check for previous-day snapshot (cross-day incremental)
        val previousDate = request.businessDate.minusDays(1)
        val previousSnapshot = snapshotRepo.findSnapshot(request.positionKey, previousDate, request.dateBasis)

        if (previousSnapshot != null) {
            calculateIncremental(request, previousSnapshot)
        } else {
            calculateFull(request)
        }
    }

    private fun aggregateMetrics(request: PositionCalcRequest): TradeMetrics? {
        return if (request.keyFormat == PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT) {
            tradeRepo.aggregateMetrics(request.positionKey, request.businessDate, request.dateBasis)
        } else {
            val dimensions = request.keyFormat.parseDimensions(request.positionKey)
            tradeRepo.aggregateMetricsByDimensions(dimensions, request.businessDate, request.dateBasis)
        }
    }

    private fun findTrades(request: PositionCalcRequest): List<TradeRecord> {
        return if (request.keyFormat == PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT) {
            tradeRepo.findTradesByPositionKeyAndDate(request.positionKey, request.businessDate, request.dateBasis)
        } else {
            val dimensions = request.keyFormat.parseDimensions(request.positionKey)
            tradeRepo.findTradesByDimensions(dimensions, request.businessDate, request.dateBasis)
        }
    }

    private fun calculateIncremental(request: PositionCalcRequest, previous: PositionSnapshot) {
        logger.debug("Incremental calculation for {} date={}", request.positionKey, request.businessDate)

        val todayMetrics = aggregateMetrics(request)

        if (todayMetrics == null) {
            // No trades today - carry forward previous snapshot
            val snapshot = PositionSnapshot(
                positionKey = request.positionKey,
                businessDate = request.businessDate,
                netQuantity = previous.netQuantity,
                grossLong = previous.grossLong,
                grossShort = previous.grossShort,
                tradeCount = previous.tradeCount,
                totalNotional = previous.totalNotional,
                calculationMethod = CalculationMethod.INCREMENTAL,
                calculationRequestId = request.requestId,
                lastSequenceNum = previous.lastSequenceNum,
                lastTradeTime = previous.lastTradeTime
            )
            snapshotRepo.saveSnapshot(snapshot, request.dateBasis, request.changeReason)

            // Copy prices from previous
            val previousPrices = priceRepo.findPricesForSnapshot(request.positionKey, previous.businessDate, request.dateBasis)
            previousPrices.forEach { prevPrice ->
                priceRepo.savePrice(
                    prevPrice.copy(businessDate = request.businessDate),
                    request.dateBasis
                )
            }
            return
        }

        // Build snapshot incrementally
        val snapshot = PositionSnapshot(
            positionKey = request.positionKey,
            businessDate = request.businessDate,
            netQuantity = previous.netQuantity + todayMetrics.netQuantity,
            grossLong = previous.grossLong + todayMetrics.grossLong,
            grossShort = previous.grossShort + todayMetrics.grossShort,
            tradeCount = previous.tradeCount + todayMetrics.tradeCount,
            totalNotional = previous.totalNotional + todayMetrics.totalNotional,
            calculationMethod = CalculationMethod.INCREMENTAL,
            calculationRequestId = request.requestId,
            lastSequenceNum = todayMetrics.lastSequenceNum,
            lastTradeTime = todayMetrics.lastTradeTime
        )
        snapshotRepo.saveSnapshot(snapshot, request.dateBasis, request.changeReason)

        // Calculate WAC incrementally
        if (request.priceMethods.contains(PriceCalculationMethod.WAC)) {
            calculateWACIncremental(request, previous)
        }
    }

    private fun calculateSameDayIncremental(request: PositionCalcRequest, existing: PositionSnapshot) {
        // Find only trades newer than what the snapshot already includes
        val newTrades = tradeRepo.findTradesAfterSequence(
            request.positionKey, request.businessDate, request.dateBasis, existing.lastSequenceNum
        )

        if (newTrades.isEmpty()) {
            logger.debug("No new trades for {} since seq={}", request.positionKey, existing.lastSequenceNum)
            return
        }

        logger.debug("Same-day incremental for {} date={}: {} new trades since seq={}",
            request.positionKey, request.businessDate, newTrades.size, existing.lastSequenceNum)

        // Update snapshot metrics incrementally
        var netQty = existing.netQuantity
        var grossLong = existing.grossLong
        var grossShort = existing.grossShort
        var tradeCount = existing.tradeCount
        var totalNotional = existing.totalNotional
        var lastSeq = existing.lastSequenceNum
        var lastTradeTime = existing.lastTradeTime

        for (trade in newTrades) {
            netQty += trade.signedQuantity
            if (trade.signedQuantity > 0) grossLong += trade.signedQuantity
            else grossShort += kotlin.math.abs(trade.signedQuantity)
            tradeCount++
            totalNotional += trade.price * kotlin.math.abs(trade.signedQuantity).toBigDecimal()
            lastSeq = trade.sequenceNum
        }

        val snapshot = PositionSnapshot(
            positionKey = request.positionKey,
            businessDate = request.businessDate,
            netQuantity = netQty,
            grossLong = grossLong,
            grossShort = grossShort,
            tradeCount = tradeCount,
            totalNotional = totalNotional,
            calculationMethod = CalculationMethod.INCREMENTAL,
            calculationRequestId = request.requestId,
            lastSequenceNum = lastSeq,
            lastTradeTime = lastTradeTime
        )
        snapshotRepo.saveSnapshot(snapshot, request.dateBasis, request.changeReason)

        // Update WAC incrementally from existing state
        if (request.priceMethods.contains(PriceCalculationMethod.WAC)) {
            val existingWac = priceRepo.findPrice(
                request.positionKey, request.businessDate, PriceCalculationMethod.WAC, request.dateBasis
            )

            var state = if (existingWac != null) {
                WacState(
                    avgPrice = existingWac.price,
                    totalCostBasis = existingWac.methodData.totalCostBasis,
                    netQuantity = existing.netQuantity,
                    lastSequence = existingWac.methodData.lastUpdatedSequence
                )
            } else {
                WacState()
            }

            for (trade in newTrades) {
                state = state.applyTrade(trade.sequenceNum, trade.signedQuantity, trade.price)
            }

            val price = PositionAveragePrice(
                positionKey = request.positionKey,
                businessDate = request.businessDate,
                priceMethod = PriceCalculationMethod.WAC,
                price = state.avgPrice,
                methodData = WacMethodData(state.totalCostBasis, state.lastSequence),
                calculatedAt = Instant.now()
            )
            priceRepo.savePrice(price, request.dateBasis)
        }
    }

    private fun calculateFull(request: PositionCalcRequest) {
        logger.debug("Full calculation for {} date={}", request.positionKey, request.businessDate)

        val metrics = aggregateMetrics(request)
        if (metrics == null) {
            logger.debug("No trades found for {} on {}", request.positionKey, request.businessDate)
            return
        }

        val snapshot = PositionSnapshot(
            positionKey = request.positionKey,
            businessDate = request.businessDate,
            netQuantity = metrics.netQuantity,
            grossLong = metrics.grossLong,
            grossShort = metrics.grossShort,
            tradeCount = metrics.tradeCount,
            totalNotional = metrics.totalNotional,
            calculationMethod = CalculationMethod.FULL_RECALC,
            calculationRequestId = request.requestId,
            lastSequenceNum = metrics.lastSequenceNum,
            lastTradeTime = metrics.lastTradeTime
        )
        snapshotRepo.saveSnapshot(snapshot, request.dateBasis, request.changeReason)

        // Calculate WAC from scratch
        if (request.priceMethods.contains(PriceCalculationMethod.WAC)) {
            calculateWACFull(request)
        }
    }

    private fun calculateWACIncremental(request: PositionCalcRequest, previousSnapshot: PositionSnapshot) {
        val previousWac = priceRepo.findPrice(
            request.positionKey,
            previousSnapshot.businessDate,
            PriceCalculationMethod.WAC,
            request.dateBasis
        )

        if (previousWac == null) {
            // Fallback to full WAC calculation
            calculateWACFull(request)
            return
        }

        // Start from previous WAC state
        var state = WacState(
            avgPrice = previousWac.price,
            totalCostBasis = previousWac.methodData.totalCostBasis,
            netQuantity = previousSnapshot.netQuantity,
            lastSequence = previousWac.methodData.lastUpdatedSequence
        )

        // Apply only today's trades
        val trades = findTrades(request)
        trades.forEach { trade ->
            state = state.applyTrade(trade.sequenceNum, trade.signedQuantity, trade.price)
        }

        val price = PositionAveragePrice(
            positionKey = request.positionKey,
            businessDate = request.businessDate,
            priceMethod = PriceCalculationMethod.WAC,
            price = state.avgPrice,
            methodData = WacMethodData(state.totalCostBasis, state.lastSequence),
            calculatedAt = Instant.now()
        )
        priceRepo.savePrice(price, request.dateBasis)
    }

    private fun calculateWACFull(request: PositionCalcRequest) {
        val trades = findTrades(request)

        var state = WacState()
        trades.forEach { trade ->
            state = state.applyTrade(trade.sequenceNum, trade.signedQuantity, trade.price)
        }

        val price = PositionAveragePrice(
            positionKey = request.positionKey,
            businessDate = request.businessDate,
            priceMethod = PriceCalculationMethod.WAC,
            price = state.avgPrice,
            methodData = WacMethodData(state.totalCostBasis, state.lastSequence),
            calculatedAt = Instant.now()
        )
        priceRepo.savePrice(price, request.dateBasis)
    }
}
