package com.positionservice.kafka

import com.positionservice.config.KafkaConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.LongDeserializer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

class KafkaConsumerWrapper(private val config: KafkaConfig) {

    private val logger = LoggerFactory.getLogger(KafkaConsumerWrapper::class.java)

    fun <K, V> createConsumer(
        topic: String,
        groupId: String,
        keyDeserializer: Class<*> = LongDeserializer::class.java,
        valueDeserializer: Class<*> = StringDeserializer::class.java,
        additionalProps: Map<String, Any> = emptyMap(),
        maxPollRecords: Int = 100
    ): ManagedConsumer<K, V> {
        val props = Properties().apply {
            put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
            put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
            put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.name)
            put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.name)
            put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
            put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false)
            put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords)
            additionalProps.forEach { (k, v) -> put(k, v) }
        }

        @Suppress("UNCHECKED_CAST")
        val consumer = KafkaConsumer<K, V>(props)
        consumer.subscribe(listOf(topic))
        logger.info("Created consumer for topic: {} with group: {}", topic, groupId)
        return ManagedConsumer(consumer)
    }
}

class ManagedConsumer<K, V>(private val consumer: KafkaConsumer<K, V>) {

    private val logger = LoggerFactory.getLogger(ManagedConsumer::class.java)
    private val running = AtomicBoolean(true)

    fun poll(timeout: Duration = Duration.ofMillis(100)): ConsumerRecords<K, V> {
        return consumer.poll(timeout)
    }

    fun commitSync() {
        consumer.commitSync()
    }

    fun isRunning(): Boolean = running.get()

    fun stop() {
        running.set(false)
    }

    fun close() {
        running.set(false)
        consumer.close()
        logger.info("Consumer closed")
    }

    fun startPolling(handler: (ConsumerRecords<K, V>) -> Unit) {
        Thread {
            try {
                while (running.get()) {
                    val records = poll()
                    if (!records.isEmpty) {
                        handler(records)
                        commitSync()
                    }
                }
            } catch (e: Exception) {
                if (running.get()) {
                    logger.error("Consumer polling error", e)
                }
            } finally {
                consumer.close()
            }
        }.apply {
            name = "kafka-consumer-thread"
            isDaemon = true
            start()
        }
    }
}
