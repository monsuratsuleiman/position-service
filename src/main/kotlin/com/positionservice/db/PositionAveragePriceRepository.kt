package com.positionservice.db

import com.positionservice.domain.DateBasis
import com.positionservice.domain.PositionAveragePrice
import com.positionservice.domain.PriceCalculationMethod
import com.positionservice.domain.WacMethodData
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

open class PositionAveragePriceRepository {

    private val jsonCodec = Json { ignoreUnknownKeys = true }

    open fun findPrice(
        positionKey: String,
        businessDate: LocalDate,
        priceMethod: PriceCalculationMethod,
        dateBasis: DateBasis
    ): PositionAveragePrice? = transaction {
        when (dateBasis) {
            DateBasis.TRADE_DATE -> findPriceTrade(positionKey, businessDate, priceMethod)
            DateBasis.SETTLEMENT_DATE -> findPriceSettled(positionKey, businessDate, priceMethod)
        }
    }

    open fun savePrice(price: PositionAveragePrice, dateBasis: DateBasis): Unit = transaction {
        when (dateBasis) {
            DateBasis.TRADE_DATE -> savePriceTrade(price)
            DateBasis.SETTLEMENT_DATE -> savePriceSettled(price)
        }
    }

    open fun findPricesForSnapshot(
        positionKey: String,
        businessDate: LocalDate,
        dateBasis: DateBasis
    ): List<PositionAveragePrice> = transaction {
        when (dateBasis) {
            DateBasis.TRADE_DATE -> findPricesForSnapshotTrade(positionKey, businessDate)
            DateBasis.SETTLEMENT_DATE -> findPricesForSnapshotSettled(positionKey, businessDate)
        }
    }

    // -- Trade Date --

    private fun findPriceTrade(positionKey: String, businessDate: LocalDate, priceMethod: PriceCalculationMethod): PositionAveragePrice? {
        val t = PositionAveragePricesTable
        return t.selectAll()
            .where { (t.positionKey eq positionKey) and (t.businessDate eq businessDate) and (t.priceMethod eq priceMethod.name) }
            .singleOrNull()
            ?.let { row -> rowToPrice(row, t.positionKey, t.businessDate, t.priceMethod, t.price, t.methodData, t.calculationVersion, t.calculatedAt) }
    }

    private fun findPriceSettled(positionKey: String, businessDate: LocalDate, priceMethod: PriceCalculationMethod): PositionAveragePrice? {
        val t = PositionAveragePricesSettledTable
        return t.selectAll()
            .where { (t.positionKey eq positionKey) and (t.businessDate eq businessDate) and (t.priceMethod eq priceMethod.name) }
            .singleOrNull()
            ?.let { row -> rowToPrice(row, t.positionKey, t.businessDate, t.priceMethod, t.price, t.methodData, t.calculationVersion, t.calculatedAt) }
    }

    private fun savePriceTrade(price: PositionAveragePrice) {
        val t = PositionAveragePricesTable
        val methodDataJson = encodeMethodData(price)

        val existing = t.selectAll()
            .where { (t.positionKey eq price.positionKey) and (t.businessDate eq price.businessDate) and (t.priceMethod eq price.priceMethod.name) }
            .singleOrNull()

        if (existing != null) {
            t.update({
                (t.positionKey eq price.positionKey) and (t.businessDate eq price.businessDate) and (t.priceMethod eq price.priceMethod.name)
            }) {
                it[t.price] = price.price
                it[t.methodData] = methodDataJson
                it[t.calculationVersion] = price.calculationVersion
                it[t.calculatedAt] = Instant.now()
            }
        } else {
            t.insert {
                it[t.positionKey] = price.positionKey
                it[t.businessDate] = price.businessDate
                it[t.priceMethod] = price.priceMethod.name
                it[t.price] = price.price
                it[t.methodData] = methodDataJson
                it[t.calculationVersion] = price.calculationVersion
                it[t.calculatedAt] = Instant.now()
            }
        }
    }

    private fun savePriceSettled(price: PositionAveragePrice) {
        val t = PositionAveragePricesSettledTable
        val methodDataJson = encodeMethodData(price)

        val existing = t.selectAll()
            .where { (t.positionKey eq price.positionKey) and (t.businessDate eq price.businessDate) and (t.priceMethod eq price.priceMethod.name) }
            .singleOrNull()

        if (existing != null) {
            t.update({
                (t.positionKey eq price.positionKey) and (t.businessDate eq price.businessDate) and (t.priceMethod eq price.priceMethod.name)
            }) {
                it[t.price] = price.price
                it[t.methodData] = methodDataJson
                it[t.calculationVersion] = price.calculationVersion
                it[t.calculatedAt] = Instant.now()
            }
        } else {
            t.insert {
                it[t.positionKey] = price.positionKey
                it[t.businessDate] = price.businessDate
                it[t.priceMethod] = price.priceMethod.name
                it[t.price] = price.price
                it[t.methodData] = methodDataJson
                it[t.calculationVersion] = price.calculationVersion
                it[t.calculatedAt] = Instant.now()
            }
        }
    }

    private fun findPricesForSnapshotTrade(positionKey: String, businessDate: LocalDate): List<PositionAveragePrice> {
        val t = PositionAveragePricesTable
        return t.selectAll()
            .where { (t.positionKey eq positionKey) and (t.businessDate eq businessDate) }
            .map { row -> rowToPrice(row, t.positionKey, t.businessDate, t.priceMethod, t.price, t.methodData, t.calculationVersion, t.calculatedAt) }
    }

    private fun findPricesForSnapshotSettled(positionKey: String, businessDate: LocalDate): List<PositionAveragePrice> {
        val t = PositionAveragePricesSettledTable
        return t.selectAll()
            .where { (t.positionKey eq positionKey) and (t.businessDate eq businessDate) }
            .map { row -> rowToPrice(row, t.positionKey, t.businessDate, t.priceMethod, t.price, t.methodData, t.calculationVersion, t.calculatedAt) }
    }

    // -- Helpers --

    private fun rowToPrice(
        row: ResultRow,
        positionKeyCol: Column<String>,
        businessDateCol: Column<LocalDate>,
        priceMethodCol: Column<String>,
        priceCol: Column<BigDecimal>,
        methodDataCol: Column<String>,
        calculationVersionCol: Column<Int>,
        calculatedAtCol: Column<Instant>
    ): PositionAveragePrice {
        val dataStr = row[methodDataCol]
        val dataMap = jsonCodec.decodeFromString<Map<String, String>>(dataStr)
        return PositionAveragePrice(
            positionKey = row[positionKeyCol],
            businessDate = row[businessDateCol],
            priceMethod = PriceCalculationMethod.valueOf(row[priceMethodCol]),
            price = row[priceCol],
            methodData = WacMethodData(
                totalCostBasis = BigDecimal(dataMap["totalCostBasis"] ?: "0"),
                lastUpdatedSequence = (dataMap["lastUpdatedSequence"] ?: "0").toLong()
            ),
            calculationVersion = row[calculationVersionCol],
            calculatedAt = row[calculatedAtCol]
        )
    }

    private fun encodeMethodData(price: PositionAveragePrice): String =
        jsonCodec.encodeToString(
            mapOf(
                "totalCostBasis" to price.methodData.totalCostBasis.toPlainString(),
                "lastUpdatedSequence" to price.methodData.lastUpdatedSequence.toString()
            )
        )
}
