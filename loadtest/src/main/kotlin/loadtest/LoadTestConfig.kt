package loadtest

import java.time.LocalDate

enum class Profile(
    val trades: Int,
    val instruments: Int,
    val books: Int,
    val counterparties: Int,
    val ratePerSecond: Int
) {
    SMOKE(100, 3, 2, 2, 100),
    MEDIUM(10_000, 10, 5, 3, 1_000),
    LARGE(100_000, 20, 10, 5, 5_000)
}

data class LoadTestConfig(
    val trades: Int,
    val instruments: Int,
    val books: Int,
    val counterparties: Int,
    val ratePerSecond: Int,
    val kafkaBootstrap: String,
    val topic: String,
    val apiBaseUrl: String,
    val businessDate: LocalDate
) {
    companion object {
        fun fromArgs(args: Array<String>): LoadTestConfig {
            val argMap = args.filter { it.startsWith("--") }
                .associate { arg ->
                    val parts = arg.removePrefix("--").split("=", limit = 2)
                    parts[0] to (parts.getOrNull(1) ?: "")
                }

            val profile = argMap["profile"]?.uppercase()?.let { Profile.valueOf(it) } ?: Profile.MEDIUM

            return LoadTestConfig(
                trades = argMap["trades"]?.toIntOrNull() ?: profile.trades,
                instruments = argMap["instruments"]?.toIntOrNull() ?: profile.instruments,
                books = argMap["books"]?.toIntOrNull() ?: profile.books,
                counterparties = argMap["counterparties"]?.toIntOrNull() ?: profile.counterparties,
                ratePerSecond = argMap["rate"]?.toIntOrNull() ?: profile.ratePerSecond,
                kafkaBootstrap = argMap["kafka"] ?: "localhost:9092",
                topic = argMap["topic"] ?: "trade-events",
                apiBaseUrl = argMap["api"] ?: "http://localhost:8080",
                businessDate = argMap["date"]?.let { LocalDate.parse(it) } ?: LocalDate.now()
            )
        }
    }
}
