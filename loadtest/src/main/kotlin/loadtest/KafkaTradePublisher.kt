package loadtest

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.TimeUnit

class KafkaTradePublisher(private val config: LoadTestConfig) {

    private val logger = LoggerFactory.getLogger(KafkaTradePublisher::class.java)

    fun publish(trades: List<GeneratedTrade>): PublishResult {
        val props = Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.RETRIES_CONFIG, 3)
            put(ProducerConfig.LINGER_MS_CONFIG, 5)
            put(ProducerConfig.BATCH_SIZE_CONFIG, 32768)
        }

        val latencies = mutableListOf<Long>()
        val startTime = System.nanoTime()
        val intervalNanos = if (config.ratePerSecond > 0) 1_000_000_000L / config.ratePerSecond else 0L

        KafkaProducer<String, String>(props).use { producer ->
            for ((index, trade) in trades.withIndex()) {
                val sendStart = System.nanoTime()

                val record = ProducerRecord(
                    config.topic,
                    trade.positionKey,  // key for partition affinity
                    trade.toJson()
                )

                producer.send(record) { _, exception ->
                    if (exception != null) {
                        logger.error("Failed to publish trade seq=${trade.sequenceNum}", exception)
                    }
                }

                val sendEnd = System.nanoTime()
                latencies.add(TimeUnit.NANOSECONDS.toMicros(sendEnd - sendStart))

                // Rate limiting
                if (intervalNanos > 0) {
                    val elapsed = sendEnd - sendStart
                    val sleepNanos = intervalNanos - elapsed
                    if (sleepNanos > 0) {
                        Thread.sleep(sleepNanos / 1_000_000, (sleepNanos % 1_000_000).toInt())
                    }
                }

                // Progress report every 1000 trades
                if ((index + 1) % 1000 == 0 || index + 1 == trades.size) {
                    val elapsedSec = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime) / 1000.0
                    val throughput = if (elapsedSec > 0) (index + 1) / elapsedSec else 0.0
                    logger.info("Published ${index + 1}/${trades.size} trades (${String.format("%.0f", throughput)} trades/sec)")
                }
            }

            producer.flush()
        }

        val totalDuration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startTime)
        latencies.sort()

        return PublishResult(
            totalTrades = trades.size,
            durationMs = totalDuration,
            avgThroughput = if (totalDuration > 0) trades.size * 1000.0 / totalDuration else 0.0,
            latencyP50Micros = percentile(latencies, 50),
            latencyP95Micros = percentile(latencies, 95),
            latencyP99Micros = percentile(latencies, 99),
            latencyMinMicros = latencies.firstOrNull() ?: 0,
            latencyMaxMicros = latencies.lastOrNull() ?: 0
        )
    }

    private fun percentile(sorted: List<Long>, p: Int): Long {
        if (sorted.isEmpty()) return 0
        val idx = ((p / 100.0) * sorted.size).toInt().coerceIn(0, sorted.size - 1)
        return sorted[idx]
    }
}

data class PublishResult(
    val totalTrades: Int,
    val durationMs: Long,
    val avgThroughput: Double,
    val latencyP50Micros: Long,
    val latencyP95Micros: Long,
    val latencyP99Micros: Long,
    val latencyMinMicros: Long,
    val latencyMaxMicros: Long
) {
    fun printSummary() {
        println()
        println("=== Publish Summary ===")
        println("Total trades:  $totalTrades")
        println("Duration:      ${durationMs}ms")
        println("Throughput:    ${String.format("%.1f", avgThroughput)} trades/sec")
        println("Latency (us):  min=${latencyMinMicros} p50=${latencyP50Micros} p95=${latencyP95Micros} p99=${latencyP99Micros} max=${latencyMaxMicros}")
        println()
    }
}
