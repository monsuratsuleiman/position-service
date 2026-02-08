package com.positionservice.db

import com.positionservice.domain.PositionConfig
import com.positionservice.domain.PositionConfigType
import com.positionservice.domain.PositionKeyFormat
import com.positionservice.domain.PriceCalculationMethod
import com.positionservice.domain.ScopePredicate
import kotlinx.serialization.json.Json
import org.jetbrains.exposed.sql.*
import org.jetbrains.exposed.sql.transactions.transaction
import java.time.Instant

open class PositionConfigRepository {

    private val scopeJson = Json {
        classDiscriminator = "type"
    }

    open fun findAll(): List<PositionConfig> = transaction {
        PositionConfigsTable.selectAll()
            .map { rowToConfig(it) }
    }

    open fun findActive(): List<PositionConfig> = transaction {
        PositionConfigsTable.selectAll()
            .where { PositionConfigsTable.active eq true }
            .map { rowToConfig(it) }
    }

    open fun findById(configId: Long): PositionConfig? = transaction {
        PositionConfigsTable.selectAll()
            .where { PositionConfigsTable.configId eq configId }
            .singleOrNull()
            ?.let { rowToConfig(it) }
    }

    open fun create(config: PositionConfig): PositionConfig = transaction {
        val id = PositionConfigsTable.insert {
            it[configType] = config.type.name
            it[name] = config.name
            it[keyFormat] = config.keyFormat.name
            it[priceMethods] = config.priceMethods.joinToString(",") { m -> m.name }
            it[scope] = scopeJson.encodeToString(ScopePredicate.serializer(), config.scope)
            it[active] = config.active
            it[createdAt] = Instant.now()
            it[updatedAt] = Instant.now()
        } get PositionConfigsTable.configId

        config.copy(configId = id)
    }

    open fun update(config: PositionConfig): Unit = transaction {
        PositionConfigsTable.update({
            PositionConfigsTable.configId eq config.configId
        }) {
            it[configType] = config.type.name
            it[name] = config.name
            it[keyFormat] = config.keyFormat.name
            it[priceMethods] = config.priceMethods.joinToString(",") { m -> m.name }
            it[scope] = scopeJson.encodeToString(ScopePredicate.serializer(), config.scope)
            it[active] = config.active
            it[updatedAt] = Instant.now()
        }
    }

    open fun deactivate(configId: Long): Unit = transaction {
        PositionConfigsTable.update({
            PositionConfigsTable.configId eq configId
        }) {
            it[active] = false
            it[updatedAt] = Instant.now()
        }
    }

    private fun rowToConfig(row: ResultRow): PositionConfig = PositionConfig(
        configId = row[PositionConfigsTable.configId],
        type = PositionConfigType.valueOf(row[PositionConfigsTable.configType]),
        name = row[PositionConfigsTable.name],
        keyFormat = PositionKeyFormat.valueOf(row[PositionConfigsTable.keyFormat]),
        priceMethods = row[PositionConfigsTable.priceMethods]
            .split(",")
            .map { PriceCalculationMethod.valueOf(it.trim()) }
            .toSet(),
        scope = scopeJson.decodeFromString(ScopePredicate.serializer(), row[PositionConfigsTable.scope]),
        active = row[PositionConfigsTable.active]
    )
}
