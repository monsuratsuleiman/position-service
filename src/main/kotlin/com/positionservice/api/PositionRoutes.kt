package com.positionservice.api

import com.positionservice.db.PositionAveragePriceRepository
import com.positionservice.db.PositionConfigRepository
import com.positionservice.db.PositionKeyRepository
import com.positionservice.db.PositionSnapshotRepository
import com.positionservice.domain.*
import com.positionservice.domain.ScopeField
import com.positionservice.domain.ScopePredicate
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable
import java.time.LocalDate

fun Application.configureRoutes(
    snapshotRepo: PositionSnapshotRepository,
    priceRepo: PositionAveragePriceRepository,
    configRepo: PositionConfigRepository,
    positionKeyRepo: PositionKeyRepository
) {
    routing {
        route("/api/v1") {
            // Health check
            get("/health") {
                call.respond(mapOf("status" to "UP"))
            }

            // -- Config CRUD --

            get("/configs") {
                val configs = configRepo.findAll()
                call.respond(configs.map { it.toResponse() })
            }

            get("/configs/{configId}") {
                val configId = call.parameters["configId"]?.toLongOrNull()
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid configId"))
                val config = configRepo.findById(configId)
                    ?: return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "Config not found"))
                call.respond(config.toResponse())
            }

            post("/configs") {
                val request = call.receive<ConfigCreateRequest>()

                val keyFormat = try {
                    PositionKeyFormat.valueOf(request.keyFormat)
                } catch (_: Exception) {
                    return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid keyFormat: ${request.keyFormat}"))
                }
                val configType = try {
                    PositionConfigType.valueOf(request.configType)
                } catch (_: Exception) {
                    return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid configType: ${request.configType}"))
                }
                val priceMethods = try {
                    request.priceMethods.map { PriceCalculationMethod.valueOf(it) }.toSet()
                } catch (_: Exception) {
                    return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid priceMethod in: ${request.priceMethods}"))
                }
                val scope = try {
                    request.scope.toDomain()
                } catch (_: Exception) {
                    return@post call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid scope"))
                }

                val config = PositionConfig(
                    type = configType,
                    name = request.name,
                    keyFormat = keyFormat,
                    priceMethods = priceMethods,
                    scope = scope
                )
                val created = configRepo.create(config)
                call.respond(HttpStatusCode.Created, created.toResponse())
            }

            put("/configs/{configId}") {
                val configId = call.parameters["configId"]?.toLongOrNull()
                    ?: return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid configId"))
                val existing = configRepo.findById(configId)
                    ?: return@put call.respond(HttpStatusCode.NotFound, mapOf("error" to "Config not found"))

                val request = call.receive<ConfigUpdateRequest>()

                val keyFormat = try {
                    PositionKeyFormat.valueOf(request.keyFormat)
                } catch (_: Exception) {
                    return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid keyFormat: ${request.keyFormat}"))
                }
                val configType = try {
                    PositionConfigType.valueOf(request.configType)
                } catch (_: Exception) {
                    return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid configType: ${request.configType}"))
                }
                val priceMethods = try {
                    request.priceMethods.map { PriceCalculationMethod.valueOf(it) }.toSet()
                } catch (_: Exception) {
                    return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid priceMethod in: ${request.priceMethods}"))
                }
                val scope = try {
                    request.scope.toDomain()
                } catch (_: Exception) {
                    return@put call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid scope"))
                }

                val updated = existing.copy(
                    type = configType,
                    name = request.name,
                    keyFormat = keyFormat,
                    priceMethods = priceMethods,
                    scope = scope,
                    active = request.active
                )
                configRepo.update(updated)
                call.respond(updated.toResponse())
            }

            delete("/configs/{configId}") {
                val configId = call.parameters["configId"]?.toLongOrNull()
                    ?: return@delete call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid configId"))
                configRepo.deactivate(configId)
                call.respond(HttpStatusCode.OK, mapOf("status" to "deactivated"))
            }

            // -- Position Keys --

            get("/position-keys") {
                val book = call.request.queryParameters["book"]
                val counterparty = call.request.queryParameters["counterparty"]
                val instrument = call.request.queryParameters["instrument"]
                val limit = call.request.queryParameters["limit"]?.toIntOrNull() ?: 500
                val results = positionKeyRepo.search(book, counterparty, instrument, limit)
                call.respond(results.map { it.toResponse() })
            }

            get("/position-keys/dimensions") {
                call.respond(PositionKeyDimensionsResponse(
                    books = positionKeyRepo.distinctBooks(),
                    counterparties = positionKeyRepo.distinctCounterparties(),
                    instruments = positionKeyRepo.distinctInstruments()
                ))
            }

            // Get current position snapshot
            get("/positions/{positionKey}/{businessDate}") {
                val positionKey = call.parameters["positionKey"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "positionKey required"))
                val businessDateStr = call.parameters["businessDate"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "businessDate required"))
                val dateBasis = call.request.queryParameters["dateBasis"]?.let { DateBasis.valueOf(it) }
                    ?: DateBasis.TRADE_DATE

                val businessDate = try {
                    LocalDate.parse(businessDateStr)
                } catch (e: Exception) {
                    return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid date format"))
                }

                val snapshot = snapshotRepo.findSnapshot(positionKey, businessDate, dateBasis)
                    ?: return@get call.respond(HttpStatusCode.NotFound, mapOf("error" to "Position not found"))

                val prices = priceRepo.findPricesForSnapshot(positionKey, businessDate, dateBasis)

                call.respond(PositionResponse(
                    positionKey = snapshot.positionKey,
                    businessDate = snapshot.businessDate.toString(),
                    dateBasis = dateBasis.name,
                    netQuantity = snapshot.netQuantity,
                    grossLong = snapshot.grossLong,
                    grossShort = snapshot.grossShort,
                    tradeCount = snapshot.tradeCount,
                    totalNotional = snapshot.totalNotional.toPlainString(),
                    calculationVersion = snapshot.calculationVersion,
                    calculatedAt = snapshot.calculatedAt.toString(),
                    calculationMethod = snapshot.calculationMethod.name,
                    lastSequenceNum = snapshot.lastSequenceNum,
                    prices = prices.map { p ->
                        PriceResponse(
                            method = p.priceMethod.name,
                            price = p.price.toPlainString(),
                            totalCostBasis = p.methodData.totalCostBasis.toPlainString()
                        )
                    }
                ))
            }

            // Get positions for a key across dates
            get("/positions/{positionKey}") {
                val positionKey = call.parameters["positionKey"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "positionKey required"))
                val dateBasis = call.request.queryParameters["dateBasis"]?.let { DateBasis.valueOf(it) }
                    ?: DateBasis.TRADE_DATE
                val fromDate = call.request.queryParameters["from"]?.let { LocalDate.parse(it) }
                val toDate = call.request.queryParameters["to"]?.let { LocalDate.parse(it) }

                val snapshots = snapshotRepo.findSnapshotsForPosition(positionKey, dateBasis, fromDate, toDate)

                call.respond(snapshots.map { s ->
                    PositionSummaryResponse(
                        positionKey = s.positionKey,
                        businessDate = s.businessDate.toString(),
                        netQuantity = s.netQuantity,
                        tradeCount = s.tradeCount,
                        calculationVersion = s.calculationVersion,
                        calculationMethod = s.calculationMethod.name
                    )
                })
            }

            // Get position history (bitemporal)
            get("/positions/{positionKey}/{businessDate}/history") {
                val positionKey = call.parameters["positionKey"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "positionKey required"))
                val businessDateStr = call.parameters["businessDate"]
                    ?: return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "businessDate required"))
                val dateBasis = call.request.queryParameters["dateBasis"]?.let { DateBasis.valueOf(it) }
                    ?: DateBasis.TRADE_DATE

                val businessDate = try {
                    LocalDate.parse(businessDateStr)
                } catch (e: Exception) {
                    return@get call.respond(HttpStatusCode.BadRequest, mapOf("error" to "Invalid date format"))
                }

                val history = snapshotRepo.findSnapshotHistory(positionKey, businessDate, dateBasis)

                call.respond(history.map { h ->
                    HistoryResponse(
                        historyId = h.historyId,
                        positionKey = h.positionKey,
                        businessDate = h.businessDate.toString(),
                        netQuantity = h.netQuantity,
                        grossLong = h.grossLong,
                        grossShort = h.grossShort,
                        tradeCount = h.tradeCount,
                        totalNotional = h.totalNotional.toPlainString(),
                        calculationVersion = h.calculationVersion,
                        calculatedAt = h.calculatedAt.toString(),
                        supersededAt = h.supersededAt?.toString(),
                        changeReason = h.changeReason,
                        previousNetQuantity = h.previousNetQuantity,
                        calculationMethod = h.calculationMethod
                    )
                })
            }
        }
    }
}

@Serializable
data class PositionResponse(
    val positionKey: String,
    val businessDate: String,
    val dateBasis: String,
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val totalNotional: String,
    val calculationVersion: Int,
    val calculatedAt: String,
    val calculationMethod: String,
    val lastSequenceNum: Long,
    val prices: List<PriceResponse>
)

@Serializable
data class PriceResponse(
    val method: String,
    val price: String,
    val totalCostBasis: String
)

@Serializable
data class PositionSummaryResponse(
    val positionKey: String,
    val businessDate: String,
    val netQuantity: Long,
    val tradeCount: Int,
    val calculationVersion: Int,
    val calculationMethod: String
)

@Serializable
data class HistoryResponse(
    val historyId: Long,
    val positionKey: String,
    val businessDate: String,
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val totalNotional: String,
    val calculationVersion: Int,
    val calculatedAt: String,
    val supersededAt: String?,
    val changeReason: String?,
    val previousNetQuantity: Long?,
    val calculationMethod: String?
)

@Serializable
data class ConfigResponse(
    val configId: Long,
    val configType: String,
    val name: String,
    val keyFormat: String,
    val priceMethods: List<String>,
    val scope: ScopePredicateDto,
    val active: Boolean
)

@Serializable
data class ConfigCreateRequest(
    val configType: String,
    val name: String,
    val keyFormat: String,
    val priceMethods: List<String>,
    val scope: ScopePredicateDto = ScopePredicateDto(type = "ALL")
)

@Serializable
data class ConfigUpdateRequest(
    val configType: String,
    val name: String,
    val keyFormat: String,
    val priceMethods: List<String>,
    val scope: ScopePredicateDto = ScopePredicateDto(type = "ALL"),
    val active: Boolean = true
)

@Serializable
data class ScopePredicateDto(
    val type: String,
    val criteria: Map<String, String>? = null
)

private fun ScopePredicateDto.toDomain(): ScopePredicate = when (type.uppercase()) {
    "ALL" -> ScopePredicate.All
    "CRITERIA" -> ScopePredicate.Criteria(
        (criteria ?: emptyMap()).map { (k, v) -> ScopeField.valueOf(k.uppercase()) to v }.toMap()
    )
    else -> throw IllegalArgumentException("Invalid scope type: $type")
}

private fun ScopePredicate.toDto(): ScopePredicateDto = when (this) {
    is ScopePredicate.All -> ScopePredicateDto(type = "ALL")
    is ScopePredicate.Criteria -> ScopePredicateDto(
        type = "CRITERIA",
        criteria = criteria.map { (k, v) -> k.name to v }.toMap()
    )
}

private fun PositionConfig.toResponse() = ConfigResponse(
    configId = configId,
    configType = type.name,
    name = name,
    keyFormat = keyFormat.name,
    priceMethods = priceMethods.map { it.name },
    scope = scope.toDto(),
    active = active
)

@Serializable
data class PositionKeyResponse(
    val positionId: Long,
    val positionKey: String,
    val configId: Long,
    val configType: String,
    val configName: String,
    val book: String?,
    val counterparty: String?,
    val instrument: String?,
    val lastTradeDate: String?,
    val lastSettlementDate: String?,
    val createdAt: String
)

@Serializable
data class PositionKeyDimensionsResponse(
    val books: List<String>,
    val counterparties: List<String>,
    val instruments: List<String>
)

private fun com.positionservice.domain.PositionKey.toResponse() = PositionKeyResponse(
    positionId = positionId,
    positionKey = positionKey,
    configId = configId,
    configType = configType,
    configName = configName,
    book = book,
    counterparty = counterparty,
    instrument = instrument,
    lastTradeDate = lastTradeDate?.toString(),
    lastSettlementDate = lastSettlementDate?.toString(),
    createdAt = createdAt.toString()
)
