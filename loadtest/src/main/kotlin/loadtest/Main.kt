package loadtest

import org.slf4j.LoggerFactory
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

private val logger = LoggerFactory.getLogger("loadtest.Main")

fun main(args: Array<String>) {
    val command = args.firstOrNull()?.removePrefix("--") ?: "full"
    val optionArgs = args.drop(if (args.firstOrNull()?.startsWith("--") == false) 1 else 0).toTypedArray()
    val config = LoadTestConfig.fromArgs(optionArgs)

    println("=== Position Service Load Test ===")
    println("Command:    $command")
    println("Trades:     ${config.trades}")
    println("Rate:       ${config.ratePerSecond}/sec")
    println("Kafka:      ${config.kafkaBootstrap}")
    println("API:        ${config.apiBaseUrl}")
    println("Date:       ${config.businessDate}")
    println("Keys:       ~${config.books * config.counterparties * config.instruments}")
    println()

    when (command) {
        "generate" -> runGenerate(config)
        "verify" -> runVerify(config)
        "full" -> runFull(config)
        else -> {
            println("Unknown command: $command")
            printUsage()
        }
    }
}

private fun runGenerate(config: LoadTestConfig): Map<String, ExpectedPosition> {
    // Ensure config exists
    ensureConfigExists(config)

    val generator = TradeGenerator(config)
    val calculator = ExpectedPositionCalculator()

    logger.info("Generating ${config.trades} trades...")
    val trades = generator.generate()

    // Feed trades to expected calculator
    for (trade in trades) {
        calculator.applyTrade(trade.positionKey, trade.signedQuantity, trade.price, trade.sequenceNum)
    }

    logger.info("Publishing ${trades.size} trades to Kafka...")
    val publisher = KafkaTradePublisher(config)
    val result = publisher.publish(trades)
    result.printSummary()

    val expected = calculator.getAllExpected()
    logger.info("Expected ${expected.size} unique position keys")
    return expected
}

private fun runVerify(config: LoadTestConfig): Boolean {
    // For standalone verify, we need to regenerate expected state
    val generator = TradeGenerator(config)
    val calculator = ExpectedPositionCalculator()
    val trades = generator.generate()
    for (trade in trades) {
        calculator.applyTrade(trade.positionKey, trade.signedQuantity, trade.price, trade.sequenceNum)
    }
    val expected = calculator.getAllExpected()

    val verifier = PositionVerifier(config, expected)
    val report = verifier.verify()
    return report.failed == 0
}

private fun runFull(config: LoadTestConfig) {
    val expected = runGenerate(config)

    val verifier = PositionVerifier(config, expected)
    verifier.waitForProcessing()
    val report = verifier.verify()

    if (report.failed == 0) {
        println("All ${report.passed} positions verified successfully!")
    } else {
        println("VERIFICATION FAILED: ${report.failed}/${report.totalPositions} positions")
        System.exit(1)
    }
}

private fun ensureConfigExists(config: LoadTestConfig) {
    try {
        val client = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()
        val request = HttpRequest.newBuilder()
            .uri(URI.create("${config.apiBaseUrl}/api/v1/configs"))
            .timeout(Duration.ofSeconds(5))
            .GET()
            .build()
        val response = client.send(request, HttpResponse.BodyHandlers.ofString())

        if (response.statusCode() == 200 && response.body().contains("configId")) {
            logger.info("Position config exists")
            return
        }

        // Create default config
        logger.info("Creating default position config...")
        val createReq = HttpRequest.newBuilder()
            .uri(URI.create("${config.apiBaseUrl}/api/v1/configs"))
            .timeout(Duration.ofSeconds(5))
            .header("Content-Type", "application/json")
            .POST(HttpRequest.BodyPublishers.ofString(
                """{"configType":"OFFICIAL","name":"Official Positions","keyFormat":"BOOK_COUNTERPARTY_INSTRUMENT","priceMethods":["WAC"],"scope":{"type":"ALL"}}"""
            ))
            .build()
        client.send(createReq, HttpResponse.BodyHandlers.ofString())
        logger.info("Default config created")
    } catch (e: Exception) {
        logger.warn("Could not check/create config (backend may not be running): ${e.message}")
    }
}

private fun printUsage() {
    println("""
Usage: ./gradlew :loadtest:run --args="<command> [options]"

Commands:
  generate   Publish trades to Kafka (no verification)
  verify     Verify positions via API (assumes data already loaded)
  full       Generate + wait + verify (default)

Options:
  --profile=SMOKE|MEDIUM|LARGE    Scale profile (default: MEDIUM)
  --trades=N                      Override trade count
  --rate=N                        Override trades/sec
  --kafka=host:port               Kafka bootstrap (default: localhost:9092)
  --api=url                       API base URL (default: http://localhost:8080)
  --date=YYYY-MM-DD               Business date (default: today)
    """.trimIndent())
}
