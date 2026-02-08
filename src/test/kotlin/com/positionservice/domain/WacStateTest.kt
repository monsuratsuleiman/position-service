package com.positionservice.domain

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.math.BigDecimal
import java.math.RoundingMode

class WacStateTest {

    private fun bd(value: String): BigDecimal = BigDecimal(value).setScale(12, RoundingMode.HALF_UP)
    private fun bd(value: Int): BigDecimal = BigDecimal(value).setScale(12, RoundingMode.HALF_UP)

    // -- ARD Appendix B Example 1: Normal Trading --

    @Nested
    @DisplayName("ARD Example 1: Normal Trading")
    inner class NormalTrading {

        @Test
        fun `buy 1000 at 150 from zero - moving away from zero`() {
            val state = WacState()
            val result = state.applyTrade(1, 1000, bd("150"))

            assertEquals(1000L, result.netQuantity)
            assertEquals(bd("150"), result.avgPrice)
        }

        @Test
        fun `buy 500 at 160 - moving away from zero, weighted average`() {
            val state = WacState(avgPrice = bd("150"), totalCostBasis = bd("150000"), netQuantity = 1000, lastSequence = 1)
            val result = state.applyTrade(2, 500, bd("160"))

            assertEquals(1500L, result.netQuantity)
            // WAC = (1000*150 + 500*160) / 1500 = 230000/1500 = 153.333333
            assertEquals(bd("153.333333333333"), result.avgPrice)
        }

        @Test
        fun `sell 400 at 155 - moving towards zero, WAC unchanged`() {
            val state = WacState(avgPrice = bd("153.333333333333"), totalCostBasis = bd("230000"), netQuantity = 1500, lastSequence = 2)
            val result = state.applyTrade(3, -400, bd("155"))

            assertEquals(1100L, result.netQuantity)
            // WAC should be UNCHANGED when moving towards zero
            assertEquals(bd("153.333333333333"), result.avgPrice)
        }

        @Test
        fun `full example 1 sequence`() {
            var state = WacState()

            // Buy 1000 @ $150
            state = state.applyTrade(1, 1000, bd("150"))
            assertEquals(1000L, state.netQuantity)
            assertEquals(bd("150"), state.avgPrice)

            // Buy 500 @ $160
            state = state.applyTrade(2, 500, bd("160"))
            assertEquals(1500L, state.netQuantity)
            assertEquals(bd("153.333333333333"), state.avgPrice)

            // Sell 400 @ $155
            state = state.applyTrade(3, -400, bd("155"))
            assertEquals(1100L, state.netQuantity)
            assertEquals(bd("153.333333333333"), state.avgPrice) // Unchanged
        }
    }

    // -- ARD Appendix B Example 2: Zero Crossing --

    @Nested
    @DisplayName("ARD Example 2: Zero Crossing")
    inner class ZeroCrossing {

        @Test
        fun `sell 800 from 500 long - crosses zero, reset to trade price`() {
            val state = WacState(avgPrice = bd("150"), totalCostBasis = bd("75000"), netQuantity = 500, lastSequence = 1)
            val result = state.applyTrade(2, -800, bd("160"))

            assertEquals(-300L, result.netQuantity)
            assertEquals(bd("160"), result.avgPrice) // Reset!
        }
    }

    // -- ARD Appendix B Example 3: Position to Zero --

    @Nested
    @DisplayName("ARD Example 3: Position to Zero")
    inner class PositionToZero {

        @Test
        fun `sell exactly closes position - avgPrice becomes 0`() {
            val state = WacState(avgPrice = bd("150"), totalCostBasis = bd("75000"), netQuantity = 500, lastSequence = 1)
            val result = state.applyTrade(2, -500, bd("155"))

            assertEquals(0L, result.netQuantity)
            assertEquals(bd(0), result.avgPrice) // Zero
            assertEquals(BigDecimal.ZERO, result.totalCostBasis)
        }
    }

    // -- Rule 4: Moving Away from Zero (buy more on long, sell more on short) --

    @Nested
    @DisplayName("Rule 4: Moving Away from Zero")
    inner class MovingAwayFromZero {

        @Test
        fun `adding to long position calculates weighted average`() {
            val state = WacState(avgPrice = bd("100"), totalCostBasis = bd("100000"), netQuantity = 1000, lastSequence = 1)
            val result = state.applyTrade(2, 500, bd("120"))

            assertEquals(1500L, result.netQuantity)
            // WAC = (100000 + 500*120) / 1500 = 160000 / 1500 = 106.666667
            assertEquals(bd("106.666666666667"), result.avgPrice)
        }

        @Test
        fun `adding to short position calculates weighted average`() {
            val state = WacState(avgPrice = bd("100"), totalCostBasis = bd("-100000"), netQuantity = -1000, lastSequence = 1)
            val result = state.applyTrade(2, -500, bd("120"))

            assertEquals(-1500L, result.netQuantity)
            // totalCost = -100000 + (120 * -500) = -160000
            // avgPrice = abs(-160000) / abs(-1500) = 106.666667
            assertTrue(java.math.BigDecimal("-160000").compareTo(result.totalCostBasis) == 0)
            assertEquals(bd("106.666666666667"), result.avgPrice)
        }
    }

    // -- Short position scenarios --

    @Nested
    @DisplayName("Short Position Scenarios")
    inner class ShortPositions {

        @Test
        fun `short sell from zero`() {
            val state = WacState()
            val result = state.applyTrade(1, -1000, bd("50"))

            assertEquals(-1000L, result.netQuantity)
            assertEquals(bd("50"), result.avgPrice)
        }

        @Test
        fun `cover short - moving towards zero, WAC unchanged`() {
            val state = WacState(avgPrice = bd("50"), totalCostBasis = bd("-50000"), netQuantity = -1000, lastSequence = 1)
            val result = state.applyTrade(2, 400, bd("45"))

            assertEquals(-600L, result.netQuantity)
            assertEquals(bd("50"), result.avgPrice) // Unchanged
        }

        @Test
        fun `short crosses to long - reset to trade price`() {
            val state = WacState(avgPrice = bd("50"), totalCostBasis = bd("-50000"), netQuantity = -1000, lastSequence = 1)
            val result = state.applyTrade(2, 1500, bd("55"))

            assertEquals(500L, result.netQuantity)
            assertEquals(bd("55"), result.avgPrice) // Reset
        }
    }

    // -- Edge cases --

    @Nested
    @DisplayName("Edge Cases")
    inner class EdgeCases {

        @Test
        fun `first trade from zero state`() {
            val state = WacState()
            val result = state.applyTrade(1, 100, bd("25.50"))

            assertEquals(100L, result.netQuantity)
            assertEquals(bd("25.50"), result.avgPrice)
            assertEquals(1L, result.lastSequence)
        }

        @Test
        fun `sequence numbers are tracked`() {
            var state = WacState()
            state = state.applyTrade(100, 1000, bd("10"))
            assertEquals(100L, state.lastSequence)

            state = state.applyTrade(200, 500, bd("20"))
            assertEquals(200L, state.lastSequence)
        }

        @Test
        fun `single share trades`() {
            var state = WacState()
            state = state.applyTrade(1, 1, bd("100"))
            assertEquals(1L, state.netQuantity)
            assertEquals(bd("100"), state.avgPrice)

            state = state.applyTrade(2, -1, bd("110"))
            assertEquals(0L, state.netQuantity)
            assertEquals(bd(0), state.avgPrice)
        }
    }
}
