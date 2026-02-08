package com.positionservice.db

import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.javatime.date
import org.jetbrains.exposed.sql.javatime.timestamp
import org.jetbrains.exposed.sql.json.jsonb

// -- Position Configs --

object PositionConfigsTable : Table("position_configs") {
    val configId = long("config_id").autoIncrement()
    val configType = varchar("config_type", 20)
    val name = varchar("name", 100)
    val keyFormat = varchar("key_format", 50)
    val priceMethods = varchar("price_methods", 200)
    val scope = jsonb<String>("scope", { it }, { it }).default("""{"type":"ALL"}""")
    val active = bool("active").default(true)
    val createdAt = timestamp("created_at")
    val updatedAt = timestamp("updated_at")

    override val primaryKey = PrimaryKey(configId)
}

// -- Position Keys --

object PositionKeysTable : Table("position_keys") {
    val positionId = long("position_id").autoIncrement()
    val positionKey = varchar("position_key", 200)
    val configId = long("config_id").default(1L)
    val configType = varchar("config_type", 20).default("OFFICIAL")
    val configName = varchar("config_name", 100).default("Official Positions")
    val book = varchar("book", 50).nullable()
    val counterparty = varchar("counterparty", 50).nullable()
    val instrument = varchar("instrument", 50).nullable()
    val lastTradeDate = date("last_trade_date").nullable()
    val lastSettlementDate = date("last_settlement_date").nullable()
    val createdAt = timestamp("created_at")
    val createdBySequence = long("created_by_sequence").nullable()

    override val primaryKey = PrimaryKey(positionId)
}

// -- Position Trades --

object PositionTradesTable : Table("position_trades") {
    val sequenceNum = long("sequence_num")
    val positionKey = varchar("position_key", 200)
    val tradeTime = timestamp("trade_time")
    val tradeDate = date("trade_date")
    val settlementDate = date("settlement_date")
    val book = varchar("book", 50)
    val counterparty = varchar("counterparty", 50)
    val instrument = varchar("instrument", 50)
    val signedQuantity = long("signed_quantity")
    val price = decimal("price", 20, 6)
    val tradeSource = varchar("source", 50)
    val sourceId = varchar("source_id", 100)
    val processedAt = timestamp("processed_at").nullable()

    override val primaryKey = PrimaryKey(sequenceNum)
}

// -- Trade Date Position Snapshots --

object PositionSnapshotsTable : Table("position_snapshots") {
    val positionKey = varchar("position_key", 200)
    val businessDate = date("business_date")
    val netQuantity = long("net_quantity")
    val grossLong = long("gross_long")
    val grossShort = long("gross_short")
    val tradeCount = integer("trade_count")
    val totalNotional = decimal("total_notional", 20, 6).nullable()
    val calculationVersion = integer("calculation_version").default(1)
    val calculatedAt = timestamp("calculated_at")
    val calculationMethod = varchar("calculation_method", 20).nullable()
    val calculationRequestId = varchar("calculation_request_id", 50).nullable()
    val lastSequenceNum = long("last_sequence_num")
    val lastTradeTime = timestamp("last_trade_time").nullable()

    override val primaryKey = PrimaryKey(positionKey, businessDate)
}

// -- Trade Date Average Prices --

object PositionAveragePricesTable : Table("position_average_prices") {
    val positionKey = varchar("position_key", 200)
    val businessDate = date("business_date")
    val priceMethod = varchar("price_method", 20)
    val price = decimal("price", 20, 12)
    val methodData = jsonb<String>("method_data", { it }, { it })
    val calculationVersion = integer("calculation_version").default(1)
    val calculatedAt = timestamp("calculated_at")

    override val primaryKey = PrimaryKey(positionKey, businessDate, priceMethod)
}

// -- Trade Date History --

object PositionSnapshotsHistoryTable : Table("position_snapshots_history") {
    val historyId = long("history_id").autoIncrement()
    val positionKey = varchar("position_key", 200)
    val businessDate = date("business_date")
    val netQuantity = long("net_quantity")
    val grossLong = long("gross_long")
    val grossShort = long("gross_short")
    val tradeCount = integer("trade_count")
    val totalNotional = decimal("total_notional", 20, 6).nullable()
    val calculationVersion = integer("calculation_version")
    val calculatedAt = timestamp("calculated_at")
    val supersededAt = timestamp("superseded_at").nullable()
    val changeReason = varchar("change_reason", 50).nullable()
    val previousNetQuantity = long("previous_net_quantity").nullable()
    val calculationRequestId = varchar("calculation_request_id", 50).nullable()
    val lastSequenceNum = long("last_sequence_num")
    val lastTradeTime = timestamp("last_trade_time").nullable()
    val calculationMethod = varchar("calculation_method", 20).nullable()

    override val primaryKey = PrimaryKey(historyId)
}

// -- Settlement Date Position Snapshots --

object PositionSnapshotsSettledTable : Table("position_snapshots_settled") {
    val positionKey = varchar("position_key", 200)
    val businessDate = date("business_date")
    val netQuantity = long("net_quantity")
    val grossLong = long("gross_long")
    val grossShort = long("gross_short")
    val tradeCount = integer("trade_count")
    val totalNotional = decimal("total_notional", 20, 6).nullable()
    val calculationVersion = integer("calculation_version").default(1)
    val calculatedAt = timestamp("calculated_at")
    val calculationMethod = varchar("calculation_method", 20).nullable()
    val calculationRequestId = varchar("calculation_request_id", 50).nullable()
    val lastSequenceNum = long("last_sequence_num")
    val lastTradeTime = timestamp("last_trade_time").nullable()

    override val primaryKey = PrimaryKey(positionKey, businessDate)
}

// -- Settlement Date Average Prices --

object PositionAveragePricesSettledTable : Table("position_average_prices_settled") {
    val positionKey = varchar("position_key", 200)
    val businessDate = date("business_date")
    val priceMethod = varchar("price_method", 20)
    val price = decimal("price", 20, 12)
    val methodData = jsonb<String>("method_data", { it }, { it })
    val calculationVersion = integer("calculation_version").default(1)
    val calculatedAt = timestamp("calculated_at")

    override val primaryKey = PrimaryKey(positionKey, businessDate, priceMethod)
}

// -- Settlement Date History --

object PositionSnapshotsSettledHistoryTable : Table("position_snapshots_settled_history") {
    val historyId = long("history_id").autoIncrement()
    val positionKey = varchar("position_key", 200)
    val businessDate = date("business_date")
    val netQuantity = long("net_quantity")
    val grossLong = long("gross_long")
    val grossShort = long("gross_short")
    val tradeCount = integer("trade_count")
    val totalNotional = decimal("total_notional", 20, 6).nullable()
    val calculationVersion = integer("calculation_version")
    val calculatedAt = timestamp("calculated_at")
    val supersededAt = timestamp("superseded_at").nullable()
    val changeReason = varchar("change_reason", 50).nullable()
    val previousNetQuantity = long("previous_net_quantity").nullable()
    val calculationRequestId = varchar("calculation_request_id", 50).nullable()
    val lastSequenceNum = long("last_sequence_num")
    val lastTradeTime = timestamp("last_trade_time").nullable()
    val calculationMethod = varchar("calculation_method", 20).nullable()

    override val primaryKey = PrimaryKey(historyId)
}
