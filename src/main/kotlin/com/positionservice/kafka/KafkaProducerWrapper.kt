package com.positionservice.kafka

import com.positionservice.config.KafkaConfig
import com.positionservice.domain.PositionCalcRequest
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.LongSerializer
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties

open class KafkaProducerWrapper(private val config: KafkaConfig) {

    private val logger = LoggerFactory.getLogger(KafkaProducerWrapper::class.java)
    private val json = Json { encodeDefaults = true }
    private var producer: KafkaProducer<Long, String>? = null

    fun start() {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 3)
            put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true)
            put(ProducerConfig.LINGER_MS_CONFIG, 1)
            put(ProducerConfig.BATCH_SIZE_CONFIG, 16384)
        }
        producer = KafkaProducer(props)
        logger.info("Kafka producer started")
    }

    open fun publishCalcRequest(request: PositionCalcRequest) {
        val value = json.encodeToString(request)
        val record = ProducerRecord(
            config.calcRequestsTopic,
            request.positionId,  // Partition key = position_id
            value
        )
        producer?.send(record) { metadata, exception ->
            if (exception != null) {
                logger.error("Failed to publish calc request: ${request.requestId}", exception)
            } else {
                logger.debug(
                    "Published calc request {} to partition {} offset {}",
                    request.requestId, metadata.partition(), metadata.offset()
                )
            }
        }
    }

    fun close() {
        producer?.close()
        logger.info("Kafka producer closed")
    }
}
