package com.positionservice.config

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.flywaydb.core.Flyway
import org.jetbrains.exposed.sql.Database
import org.slf4j.LoggerFactory
import javax.sql.DataSource

class DatabaseFactory(private val config: DatabaseConfig) {

    private val logger = LoggerFactory.getLogger(DatabaseFactory::class.java)
    private lateinit var dataSource: HikariDataSource

    fun connect(): Database {
        dataSource = createHikariDataSource()
        runMigrations(dataSource)
        return Database.connect(dataSource)
    }

    fun close() {
        if (::dataSource.isInitialized) {
            dataSource.close()
        }
    }

    fun getDataSource(): DataSource = dataSource

    private fun createHikariDataSource(): HikariDataSource {
        val hikariConfig = HikariConfig().apply {
            jdbcUrl = config.url
            username = config.user
            password = config.password
            maximumPoolSize = config.maxPoolSize
            isAutoCommit = false
            transactionIsolation = "TRANSACTION_READ_COMMITTED"
            validate()
        }
        logger.info("Connecting to database: ${config.url}")
        return HikariDataSource(hikariConfig)
    }

    private fun runMigrations(dataSource: DataSource) {
        logger.info("Running Flyway migrations...")
        val migrationCount = Flyway.configure()
            .dataSource(dataSource)
            .locations("classpath:db/migration")
            .load()
            .migrate()
            .migrationsExecuted
        logger.info("Flyway migrations complete: $migrationCount migrations executed")
    }
}
