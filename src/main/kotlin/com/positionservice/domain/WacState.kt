package com.positionservice.domain

import java.math.BigDecimal
import java.math.MathContext
import java.math.RoundingMode
import kotlin.math.abs

data class WacState(
    val avgPrice: BigDecimal = BigDecimal.ZERO,
    val totalCostBasis: BigDecimal = BigDecimal.ZERO,
    val netQuantity: Long = 0,
    val lastSequence: Long = 0
) {
    companion object {
        private val MATH_CONTEXT = MathContext(20, RoundingMode.HALF_UP)
        private val PRICE_SCALE = 12
    }

    fun applyTrade(seq: Long, qty: Long, price: BigDecimal): WacState {
        val newQty = netQuantity + qty

        return when {
            // Rule 1: Position crosses zero → Reset to trade price
            crossesZero(netQuantity, newQty) -> {
                val newCost = price * newQty.toBigDecimal()
                WacState(
                    avgPrice = price.setScale(PRICE_SCALE, RoundingMode.HALF_UP),
                    totalCostBasis = newCost,
                    netQuantity = newQty,
                    lastSequence = seq
                )
            }

            // Rule 2: Position equals zero → avgPrice = 0
            newQty == 0L -> WacState(
                avgPrice = BigDecimal.ZERO.setScale(PRICE_SCALE),
                totalCostBasis = BigDecimal.ZERO,
                netQuantity = 0,
                lastSequence = seq
            )

            // Rule 3: Moving towards zero → WAC unchanged
            movingTowardsZero(netQuantity, qty) -> {
                val newCost = totalCostBasis + avgPrice * qty.toBigDecimal()
                WacState(
                    avgPrice = avgPrice,
                    totalCostBasis = newCost,
                    netQuantity = newQty,
                    lastSequence = seq
                )
            }

            // Rule 4: Moving away from zero → Weighted average
            // Special case: first trade from flat position uses trade price directly
            else -> {
                if (netQuantity == 0L) {
                    val newCost = price * newQty.toBigDecimal()
                    WacState(
                        avgPrice = price.setScale(PRICE_SCALE, RoundingMode.HALF_UP),
                        totalCostBasis = newCost,
                        netQuantity = newQty,
                        lastSequence = seq
                    )
                } else {
                    val newCost = totalCostBasis + price * qty.toBigDecimal()
                    val newAvg = newCost.abs().divide(abs(newQty).toBigDecimal(), PRICE_SCALE, RoundingMode.HALF_UP)
                    WacState(
                        avgPrice = newAvg,
                        totalCostBasis = newCost,
                        netQuantity = newQty,
                        lastSequence = seq
                    )
                }
            }
        }
    }

    private fun crossesZero(old: Long, new: Long): Boolean =
        (old > 0 && new < 0) || (old < 0 && new > 0)

    private fun movingTowardsZero(current: Long, trade: Long): Boolean =
        (current > 0 && trade < 0) || (current < 0 && trade > 0)
}
