package loadtest

import kotlinx.serialization.Serializable
import kotlinx.serialization.encodeToString
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.jsonArray
import kotlinx.serialization.json.jsonObject
import kotlinx.serialization.json.jsonPrimitive
import kotlinx.serialization.json.long
import org.slf4j.LoggerFactory
import java.io.File
import java.math.BigDecimal
import java.net.URI
import java.net.URLEncoder
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.time.LocalDate

@Serializable
data class VerificationResult(
    val positionKey: String,
    val passed: Boolean,
    val failures: List<String> = emptyList(),
    val expected: ExpectedValues? = null,
    val actual: ActualValues? = null
)

@Serializable
data class ExpectedValues(
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val wacPrice: String,
    val totalCostBasis: String
)

@Serializable
data class ActualValues(
    val netQuantity: Long,
    val grossLong: Long,
    val grossShort: Long,
    val tradeCount: Int,
    val wacPrice: String?,
    val totalCostBasis: String?
)

@Serializable
data class VerificationReport(
    val totalPositions: Int,
    val passed: Int,
    val failed: Int,
    val durationMs: Long,
    val results: List<VerificationResult>
)

class PositionVerifier(
    private val config: LoadTestConfig,
    private val expectedPositions: Map<String, ExpectedPosition>
) {
    private val logger = LoggerFactory.getLogger(PositionVerifier::class.java)
    private val json = Json { ignoreUnknownKeys = true }
    private val client = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(10))
        .build()

    fun waitForProcessing(maxWaitSeconds: Int = 120) {
        logger.info("Waiting for backend to process all ${expectedPositions.size} positions (max ${maxWaitSeconds}s)...")

        var waited = 0
        while (waited < maxWaitSeconds) {
            var readyCount = 0
            var laggard = ""

            for ((key, expected) in expectedPositions) {
                val actual = pollTradeCount(key)
                if (actual >= expected.tradeCount) {
                    readyCount++
                } else if (laggard.isEmpty()) {
                    laggard = "$key has $actual/${expected.tradeCount} trades"
                }
            }

            if (readyCount == expectedPositions.size) {
                logger.info("All ${expectedPositions.size} positions fully processed")
                return
            }

            if (waited % 3 == 0) {
                logger.info("  $readyCount/${expectedPositions.size} positions ready — $laggard")
            }

            Thread.sleep(1000)
            waited++
        }

        logger.warn("Timeout waiting for processing after ${maxWaitSeconds}s — proceeding with verification anyway")
    }

    private fun pollTradeCount(positionKey: String): Long {
        return try {
            val encodedKey = URLEncoder.encode(positionKey, "UTF-8")
            val url = "${config.apiBaseUrl}/api/v1/positions/$encodedKey/${config.businessDate}?dateBasis=TRADE_DATE"
            val request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(5))
                .GET()
                .build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())
            if (response.statusCode() == 200) {
                val body = json.decodeFromString<JsonObject>(response.body())
                body["tradeCount"]?.jsonPrimitive?.long ?: 0
            } else 0
        } catch (_: Exception) { 0 }
    }

    fun verify(): VerificationReport {
        val startTime = System.currentTimeMillis()
        val results = mutableListOf<VerificationResult>()

        for ((positionKey, expected) in expectedPositions) {
            val result = verifyPosition(positionKey, expected)
            results.add(result)

            if (!result.passed) {
                logger.warn("FAIL $positionKey: ${result.failures}")
            }
        }

        val durationMs = System.currentTimeMillis() - startTime
        val passed = results.count { it.passed }
        val failed = results.count { !it.passed }

        val report = VerificationReport(
            totalPositions = results.size,
            passed = passed,
            failed = failed,
            durationMs = durationMs,
            results = results
        )

        printReport(report)
        saveReport(report)

        return report
    }

    private fun verifyPosition(positionKey: String, expected: ExpectedPosition): VerificationResult {
        val encodedKey = URLEncoder.encode(positionKey, "UTF-8")
        val url = "${config.apiBaseUrl}/api/v1/positions/$encodedKey/${config.businessDate}?dateBasis=TRADE_DATE"

        try {
            val request = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build()
            val response = client.send(request, HttpResponse.BodyHandlers.ofString())

            if (response.statusCode() != 200) {
                return VerificationResult(
                    positionKey = positionKey,
                    passed = false,
                    failures = listOf("HTTP ${response.statusCode()}: ${response.body()}")
                )
            }

            val body = json.decodeFromString<JsonObject>(response.body())
            val failures = mutableListOf<String>()

            val actualNetQty = body["netQuantity"]!!.jsonPrimitive.long
            val actualGrossLong = body["grossLong"]!!.jsonPrimitive.long
            val actualGrossShort = body["grossShort"]!!.jsonPrimitive.long
            val actualTradeCount = body["tradeCount"]!!.jsonPrimitive.long.toInt()

            if (actualNetQty != expected.netQuantity) {
                failures.add("netQuantity: expected=${expected.netQuantity} actual=$actualNetQty")
            }
            if (actualGrossLong != expected.grossLong) {
                failures.add("grossLong: expected=${expected.grossLong} actual=$actualGrossLong")
            }
            if (actualGrossShort != expected.grossShort) {
                failures.add("grossShort: expected=${expected.grossShort} actual=$actualGrossShort")
            }
            if (actualTradeCount != expected.tradeCount) {
                failures.add("tradeCount: expected=${expected.tradeCount} actual=$actualTradeCount")
            }

            // Check WAC price from prices array
            val prices = body["prices"]?.jsonArray
            val wacPrice = prices?.firstOrNull {
                it.jsonObject["method"]?.jsonPrimitive?.content == "WAC"
            }?.jsonObject

            var actualWacPrice: String? = null
            var actualTotalCostBasis: String? = null

            if (wacPrice != null) {
                actualWacPrice = wacPrice["price"]?.jsonPrimitive?.content
                actualTotalCostBasis = wacPrice["totalCostBasis"]?.jsonPrimitive?.content

                if (actualWacPrice != null) {
                    val actualPriceBd = BigDecimal(actualWacPrice)
                    val diff = (actualPriceBd - expected.wacPrice).abs()
                    if (diff > BigDecimal("0.000001")) {
                        failures.add("wacPrice: expected=${expected.wacPrice.toPlainString()} actual=$actualWacPrice (diff=$diff)")
                    }
                }

                if (actualTotalCostBasis != null) {
                    val actualCostBd = BigDecimal(actualTotalCostBasis)
                    val diff = (actualCostBd - expected.totalCostBasis).abs()
                    if (diff > BigDecimal("0.01")) {
                        failures.add("totalCostBasis: expected=${expected.totalCostBasis.toPlainString()} actual=$actualTotalCostBasis (diff=$diff)")
                    }
                }
            } else {
                failures.add("No WAC price found in response")
            }

            return VerificationResult(
                positionKey = positionKey,
                passed = failures.isEmpty(),
                failures = failures,
                expected = ExpectedValues(
                    netQuantity = expected.netQuantity,
                    grossLong = expected.grossLong,
                    grossShort = expected.grossShort,
                    tradeCount = expected.tradeCount,
                    wacPrice = expected.wacPrice.toPlainString(),
                    totalCostBasis = expected.totalCostBasis.toPlainString()
                ),
                actual = ActualValues(
                    netQuantity = actualNetQty,
                    grossLong = actualGrossLong,
                    grossShort = actualGrossShort,
                    tradeCount = actualTradeCount,
                    wacPrice = actualWacPrice,
                    totalCostBasis = actualTotalCostBasis
                )
            )
        } catch (e: Exception) {
            return VerificationResult(
                positionKey = positionKey,
                passed = false,
                failures = listOf("Error: ${e.message}")
            )
        }
    }

    private fun printReport(report: VerificationReport) {
        println()
        println("=== Verification Report ===")
        println("Total positions: ${report.totalPositions}")
        if (report.failed == 0) {
            println("Passed: ${report.passed}/${report.totalPositions}")
        } else {
            println("Passed: ${report.passed}")
            println("Failed: ${report.failed}")
            println()
            println("--- Failures ---")
            for (result in report.results.filter { !it.passed }) {
                println("  ${result.positionKey}:")
                for (failure in result.failures) {
                    println("    - $failure")
                }
            }
        }
        println("Duration: ${report.durationMs / 1000.0}s")
        println()
    }

    private fun saveReport(report: VerificationReport) {
        val dir = File("loadtest/results")
        dir.mkdirs()
        val file = File(dir, "verification-report.json")
        val jsonStr = Json { prettyPrint = true }.encodeToString(report)
        file.writeText(jsonStr)
        println("Written to: ${file.path}")
    }
}
