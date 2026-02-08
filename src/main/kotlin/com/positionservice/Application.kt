package com.positionservice

import com.positionservice.api.configureRoutes
import com.positionservice.calculation.PositionCalculationEngine
import com.positionservice.config.*
import com.positionservice.consumer.TradeEventConsumer
import com.positionservice.domain.PositionCalcRequest
import com.positionservice.domain.TradeEvent
import com.positionservice.kafka.KafkaConsumerWrapper
import com.positionservice.kafka.KafkaProducerWrapper
import io.ktor.serialization.kotlinx.json.*
import io.ktor.server.application.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.plugins.statuspages.*
import io.ktor.http.*
import io.ktor.server.response.*
import kotlinx.serialization.json.Json
import com.positionservice.db.*
import org.slf4j.LoggerFactory

private val logger = LoggerFactory.getLogger("com.positionservice.Application")

fun main() {
    embeddedServer(Netty, port = 8080, module = Application::module).start(wait = true)
}

fun Application.module() {
    val appConfig = loadAppConfig()

    // Content negotiation (JSON)
    install(ContentNegotiation) {
        json(Json {
            prettyPrint = true
            encodeDefaults = true
            ignoreUnknownKeys = true
        })
    }

    // Status pages (error handling)
    install(StatusPages) {
        exception<Throwable> { call, cause ->
            logger.error("Unhandled exception", cause)
            call.respond(HttpStatusCode.InternalServerError, mapOf("error" to (cause.message ?: "Internal error")))
        }
    }

    // Create dependencies manually
    val dbFactory = DatabaseFactory(appConfig.database)
    dbFactory.connect()

    val tradeRepo = PositionTradeRepository()
    val positionKeyRepo = PositionKeyRepository()
    val snapshotRepo = PositionSnapshotRepository()
    val priceRepo = PositionAveragePriceRepository()
    val configRepo = PositionConfigRepository()

    val kafkaProducer = KafkaProducerWrapper(appConfig.kafka)
    kafkaProducer.start()

    val kafkaConsumerWrapper = KafkaConsumerWrapper(appConfig.kafka)
    val calcEngine = PositionCalculationEngine(tradeRepo, snapshotRepo, priceRepo)
    val tradeEventConsumer = TradeEventConsumer(tradeRepo, positionKeyRepo, kafkaProducer, configRepo)

    // Start consumers
    startTradeEventConsumer(kafkaConsumerWrapper, tradeEventConsumer, appConfig)
    startCalculationWorker(kafkaConsumerWrapper, calcEngine, appConfig)

    // Configure routes
    configureRoutes(snapshotRepo, priceRepo, configRepo, positionKeyRepo)

    // Shutdown hook
    environment.monitor.subscribe(ApplicationStopped) {
        logger.info("Application stopping...")
        kafkaProducer.close()
        dbFactory.close()
    }

    logger.info("Position Service started on port 8080")
}

private fun Application.startTradeEventConsumer(
    consumerWrapper: KafkaConsumerWrapper,
    tradeEventConsumer: TradeEventConsumer,
    config: AppConfig
) {
    val consumer = consumerWrapper.createConsumer<String, String>(
        topic = config.kafka.tradeEventsTopic,
        groupId = "${config.kafka.consumerGroup}-trades",
        keyDeserializer = org.apache.kafka.common.serialization.StringDeserializer::class.java,
        maxPollRecords = 5000
    )

    consumer.startPolling { records ->
        val trades = records.mapNotNull { record ->
            try {
                deserializeTradeEvent(record.value())
            } catch (e: Exception) {
                logger.error("Error deserializing trade event: ${record.value()}", e)
                null
            }
        }
        if (trades.isNotEmpty()) {
            tradeEventConsumer.processTradesBatch(trades)
        }
    }
}

private fun Application.startCalculationWorker(
    consumerWrapper: KafkaConsumerWrapper,
    calcEngine: PositionCalculationEngine,
    config: AppConfig
) {
    val consumer = consumerWrapper.createConsumer<Long, String>(
        topic = config.kafka.calcRequestsTopic,
        groupId = "${config.kafka.consumerGroup}-calc"
    )

    val json = Json { ignoreUnknownKeys = true }

    consumer.startPolling { records ->
        records.forEach { record ->
            try {
                val request = json.decodeFromString<PositionCalcRequest>(record.value())
                calcEngine.calculatePosition(request)
            } catch (e: Exception) {
                logger.error("Error processing calc request: ${record.value()}", e)
            }
        }
    }
}

private fun deserializeTradeEvent(value: String): TradeEvent? {
    return try {
        val json = Json { ignoreUnknownKeys = true }
        val map = json.decodeFromString<Map<String, kotlinx.serialization.json.JsonElement>>(value)
        TradeEvent(
            sequenceNum = map["sequenceNum"].toString().trim('"').toLong(),
            book = map["book"].toString().trim('"'),
            counterparty = map["counterparty"].toString().trim('"'),
            instrument = map["instrument"].toString().trim('"'),
            signedQuantity = map["signedQuantity"].toString().trim('"').toLong(),
            price = map["price"].toString().trim('"').toBigDecimal(),
            tradeTime = java.time.Instant.parse(map["tradeTime"].toString().trim('"')),
            tradeDate = java.time.LocalDate.parse(map["tradeDate"].toString().trim('"')),
            settlementDate = java.time.LocalDate.parse(map["settlementDate"].toString().trim('"')),
            source = map["source"].toString().trim('"'),
            sourceId = map["sourceId"].toString().trim('"')
        )
    } catch (e: Exception) {
        logger.error("Failed to deserialize trade event: $value", e)
        null
    }
}

private fun Application.loadAppConfig(): AppConfig {
    val dbUrl = environment.config.propertyOrNull("database.url")?.getString() ?: "jdbc:postgresql://localhost:5432/position_service"
    val dbUser = environment.config.propertyOrNull("database.user")?.getString() ?: "position"
    val dbPassword = environment.config.propertyOrNull("database.password")?.getString() ?: "position123"
    val dbMaxPool = environment.config.propertyOrNull("database.maxPoolSize")?.getString()?.toIntOrNull() ?: 20

    val kafkaBootstrap = environment.config.propertyOrNull("kafka.bootstrapServers")?.getString() ?: "localhost:9092"
    val kafkaGroup = environment.config.propertyOrNull("kafka.consumerGroup")?.getString() ?: "position-service"
    val tradesTopic = environment.config.propertyOrNull("kafka.tradeEventsTopic")?.getString() ?: "trade-events"
    val calcTopic = environment.config.propertyOrNull("kafka.calcRequestsTopic")?.getString() ?: "position-calculation-requests"

    return AppConfig(
        database = DatabaseConfig(dbUrl, dbUser, dbPassword, dbMaxPool),
        kafka = KafkaConfig(kafkaBootstrap, kafkaGroup, tradesTopic, calcTopic)
    )
}
