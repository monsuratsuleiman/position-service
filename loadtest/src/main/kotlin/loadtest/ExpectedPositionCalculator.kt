package loadtest

import java.math.BigDecimal
import java.math.RoundingMode
import kotlin.math.abs

data class ExpectedPosition(
    val positionKey: String,
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val wacPrice: BigDecimal,
    val totalCostBasis: BigDecimal,
    val totalNotional: BigDecimal,
    val lastSequenceNum: Long
)

class ExpectedPositionCalculator {

    companion object {
        private const val PRICE_SCALE = 12
    }

    private data class PositionState(
        var netQuantity: Long = 0,
        var grossLong: Long = 0,
        var grossShort: Long = 0,
        var tradeCount: Int = 0,
        var avgPrice: BigDecimal = BigDecimal.ZERO,
        var totalCostBasis: BigDecimal = BigDecimal.ZERO,
        var totalNotional: BigDecimal = BigDecimal.ZERO,
        var lastSequenceNum: Long = 0
    )

    private val states = mutableMapOf<String, PositionState>()

    fun applyTrade(positionKey: String, qty: Long, price: BigDecimal, seq: Long) {
        val state = states.getOrPut(positionKey) { PositionState() }

        // Update snapshot metrics
        if (qty > 0) {
            state.grossLong += qty
        } else {
            state.grossShort += abs(qty)
        }
        state.tradeCount++
        state.totalNotional += price * abs(qty).toBigDecimal()
        state.lastSequenceNum = seq

        // Apply WAC logic (mirrors WacState.kt exactly)
        val oldQty = state.netQuantity
        val newQty = oldQty + qty

        when {
            // Rule 1: Position crosses zero -> Reset to trade price
            crossesZero(oldQty, newQty) -> {
                val newCost = price * newQty.toBigDecimal()
                state.avgPrice = price.setScale(PRICE_SCALE, RoundingMode.HALF_UP)
                state.totalCostBasis = newCost
            }

            // Rule 2: Position equals zero -> avgPrice = 0
            newQty == 0L -> {
                state.avgPrice = BigDecimal.ZERO.setScale(PRICE_SCALE)
                state.totalCostBasis = BigDecimal.ZERO
            }

            // Rule 3: Moving towards zero -> WAC unchanged
            movingTowardsZero(oldQty, qty) -> {
                val newCost = state.totalCostBasis + state.avgPrice * qty.toBigDecimal()
                state.totalCostBasis = newCost
            }

            // Rule 4: Moving away from zero -> Weighted average
            else -> {
                if (oldQty == 0L) {
                    // First trade from flat: use trade price directly
                    val newCost = price * newQty.toBigDecimal()
                    state.avgPrice = price.setScale(PRICE_SCALE, RoundingMode.HALF_UP)
                    state.totalCostBasis = newCost
                } else {
                    val newCost = state.totalCostBasis + price * qty.toBigDecimal()
                    val newAvg = newCost.abs().divide(abs(newQty).toBigDecimal(), PRICE_SCALE, RoundingMode.HALF_UP)
                    state.avgPrice = newAvg
                    state.totalCostBasis = newCost
                }
            }
        }

        state.netQuantity = newQty
    }

    fun getExpected(positionKey: String): ExpectedPosition? {
        val state = states[positionKey] ?: return null
        return ExpectedPosition(
            positionKey = positionKey,
            netQuantity = state.netQuantity,
            grossLong = state.grossLong,
            grossShort = state.grossShort,
            tradeCount = state.tradeCount,
            wacPrice = state.avgPrice,
            totalCostBasis = state.totalCostBasis,
            totalNotional = state.totalNotional,
            lastSequenceNum = state.lastSequenceNum
        )
    }

    fun getAllExpected(): Map<String, ExpectedPosition> =
        states.keys.associateWith { getExpected(it)!! }

    private fun crossesZero(old: Long, new: Long): Boolean =
        (old > 0 && new < 0) || (old < 0 && new > 0)

    private fun movingTowardsZero(current: Long, trade: Long): Boolean =
        (current > 0 && trade < 0) || (current < 0 && trade > 0)
}
