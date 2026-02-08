package com.positionservice.config

data class AppConfig(
    val database: DatabaseConfig,
    val kafka: KafkaConfig
)

data class DatabaseConfig(
    val url: String,
    val user: String,
    val password: String,
    val maxPoolSize: Int = 20
)

data class KafkaConfig(
    val bootstrapServers: String,
    val consumerGroup: String,
    val tradeEventsTopic: String,
    val calcRequestsTopic: String
)
