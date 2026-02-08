package com.positionservice.domain

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

@Serializable
sealed class ScopePredicate {
    abstract fun matches(trade: TradeEvent): Boolean

    @Serializable
    @SerialName("ALL")
    data object All : ScopePredicate() {
        override fun matches(trade: TradeEvent): Boolean = true
    }

    @Serializable
    @SerialName("CRITERIA")
    data class Criteria(val criteria: Map<ScopeField, String>) : ScopePredicate() {
        override fun matches(trade: TradeEvent): Boolean =
            criteria.all { (field, value) -> field.extractFrom(trade) == value }
    }
}

data class PositionConfig(
    val configId: Long = 1L,
    val type: PositionConfigType = PositionConfigType.OFFICIAL,
    val name: String = "Official Positions",
    val keyFormat: PositionKeyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT,
    val priceMethods: Set<PriceCalculationMethod> = setOf(PriceCalculationMethod.WAC),
    val scope: ScopePredicate = ScopePredicate.All,
    val active: Boolean = true
) {
    companion object {
        fun seed() = PositionConfig()
    }
}

data class TradeEvent(
    val sequenceNum: Long,
    val book: String,
    val counterparty: String,
    val instrument: String,
    val signedQuantity: Long,
    val price: BigDecimal,
    val tradeTime: Instant,
    val tradeDate: LocalDate,
    val settlementDate: LocalDate,
    val source: String,
    val sourceId: String
)

data class PositionKey(
    val positionId: Long = 0,
    val positionKey: String,
    val configId: Long,
    val configType: String,
    val configName: String,
    val book: String?,
    val counterparty: String?,
    val instrument: String?,
    val lastTradeDate: LocalDate? = null,
    val lastSettlementDate: LocalDate? = null,
    val createdAt: Instant = Instant.now(),
    val createdBySequence: Long? = null
)

data class PositionSnapshot(
    val positionKey: String,
    val businessDate: LocalDate,
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val totalNotional: BigDecimal,
    val calculationVersion: Int = 1,
    val calculatedAt: Instant = Instant.now(),
    val calculationMethod: CalculationMethod = CalculationMethod.FULL_RECALC,
    val calculationRequestId: String? = null,
    val lastSequenceNum: Long,
    val lastTradeTime: Instant? = null
)

data class PositionAveragePrice(
    val positionKey: String,
    val businessDate: LocalDate,
    val priceMethod: PriceCalculationMethod,
    val price: BigDecimal,
    val methodData: WacMethodData,
    val calculationVersion: Int = 1,
    val calculatedAt: Instant = Instant.now()
)

data class WacMethodData(
    val totalCostBasis: BigDecimal,
    val lastUpdatedSequence: Long
)

data class TradeMetrics(
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val totalNotional: BigDecimal,
    val lastSequenceNum: Long,
    val lastTradeTime: Instant? = null
)

@Serializable
data class PositionCalcRequest(
    val requestId: String,
    val positionId: Long,
    val positionKey: String,
    val dateBasis: DateBasis,
    @Serializable(with = LocalDateSerializer::class)
    val businessDate: LocalDate,
    val priceMethods: Set<PriceCalculationMethod>,
    val triggeringTradeSequence: Long,
    val changeReason: ChangeReason = ChangeReason.INITIAL,
    val keyFormat: PositionKeyFormat = PositionKeyFormat.BOOK_COUNTERPARTY_INSTRUMENT
)

data class UpsertResult(
    val positionId: Long,
    val lastTradeDate: LocalDate?,
    val lastSettlementDate: LocalDate?
)
