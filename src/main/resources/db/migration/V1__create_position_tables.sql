-- Position ID management with cached last dates
CREATE TABLE position_keys (
    position_id BIGSERIAL PRIMARY KEY,
    position_key VARCHAR(200) NOT NULL,
    config_id BIGINT NOT NULL DEFAULT 1,
    config_type VARCHAR(20) NOT NULL DEFAULT 'OFFICIAL',
    config_name VARCHAR(100) NOT NULL DEFAULT 'Official Positions',

    -- Dimensions
    book VARCHAR(50) NOT NULL,
    counterparty VARCHAR(50) NOT NULL,
    instrument VARCHAR(50) NOT NULL,

    -- Cached optimization (avoid MAX queries)
    last_trade_date DATE,
    last_settlement_date DATE,

    -- Metadata
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    created_by_sequence BIGINT,

    UNIQUE (position_key, config_id)
);

CREATE INDEX idx_position_keys_config ON position_keys(config_id);
CREATE INDEX idx_position_keys_book_cpty_inst ON position_keys(book, counterparty, instrument);

-- Trade event log
CREATE TABLE position_trades (
    sequence_num BIGINT PRIMARY KEY,
    position_key VARCHAR(200) NOT NULL,

    -- Temporal
    trade_time TIMESTAMP NOT NULL,
    trade_date DATE NOT NULL,
    settlement_date DATE NOT NULL,

    -- Dimensions
    book VARCHAR(50) NOT NULL,
    counterparty VARCHAR(50) NOT NULL,
    instrument VARCHAR(50) NOT NULL,

    -- Economics
    signed_quantity BIGINT NOT NULL CHECK (signed_quantity != 0),
    price DECIMAL(20,6) NOT NULL CHECK (price > 0),

    -- Metadata
    source VARCHAR(50) NOT NULL,
    source_id VARCHAR(100) NOT NULL,
    processed_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_trades_position_trade_date ON position_trades(position_key, trade_date);
CREATE INDEX idx_trades_position_settlement_date ON position_trades(position_key, settlement_date);

-- Trade date positions (current state)
CREATE TABLE position_snapshots (
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,

    -- Metrics
    net_quantity BIGINT NOT NULL,
    gross_long BIGINT NOT NULL,
    gross_short BIGINT NOT NULL,
    trade_count INTEGER NOT NULL,
    total_notional DECIMAL(20,6),

    -- Versioning
    calculation_version INTEGER NOT NULL DEFAULT 1,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    calculation_method VARCHAR(20),
    calculation_request_id VARCHAR(50),

    -- Tracking
    last_sequence_num BIGINT NOT NULL,
    last_trade_time TIMESTAMP,

    PRIMARY KEY (position_key, business_date)
);

CREATE INDEX idx_snapshots_calculated ON position_snapshots(calculated_at);

-- Trade date average prices
CREATE TABLE position_average_prices (
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,
    price_method VARCHAR(20) NOT NULL,

    price DECIMAL(20,6) NOT NULL,
    method_data JSONB NOT NULL,

    calculation_version INTEGER NOT NULL DEFAULT 1,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (position_key, business_date, price_method),
    FOREIGN KEY (position_key, business_date)
        REFERENCES position_snapshots(position_key, business_date)
);

-- Trade date history (bitemporal)
CREATE TABLE position_snapshots_history (
    history_id BIGSERIAL PRIMARY KEY,
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,

    -- Metrics
    net_quantity BIGINT NOT NULL,
    gross_long BIGINT NOT NULL,
    gross_short BIGINT NOT NULL,
    trade_count INTEGER NOT NULL,
    total_notional DECIMAL(20,6),

    -- Bitemporal
    calculation_version INTEGER NOT NULL,
    calculated_at TIMESTAMP NOT NULL,
    superseded_at TIMESTAMP,

    -- Audit
    change_reason VARCHAR(50),
    previous_net_quantity BIGINT,
    calculation_request_id VARCHAR(50),

    -- Tracking
    last_sequence_num BIGINT NOT NULL,
    last_trade_time TIMESTAMP,
    calculation_method VARCHAR(20)
);

CREATE INDEX idx_history_position_date ON position_snapshots_history(position_key, business_date);
CREATE INDEX idx_history_current ON position_snapshots_history(position_key, business_date)
    WHERE superseded_at IS NULL;

-- Settlement date positions (current state)
CREATE TABLE position_snapshots_settled (
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,

    -- Metrics
    net_quantity BIGINT NOT NULL,
    gross_long BIGINT NOT NULL,
    gross_short BIGINT NOT NULL,
    trade_count INTEGER NOT NULL,
    total_notional DECIMAL(20,6),

    -- Versioning
    calculation_version INTEGER NOT NULL DEFAULT 1,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    calculation_method VARCHAR(20),
    calculation_request_id VARCHAR(50),

    -- Tracking
    last_sequence_num BIGINT NOT NULL,
    last_trade_time TIMESTAMP,

    PRIMARY KEY (position_key, business_date)
);

CREATE INDEX idx_snapshots_settled_calculated ON position_snapshots_settled(calculated_at);

-- Settlement date average prices
CREATE TABLE position_average_prices_settled (
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,
    price_method VARCHAR(20) NOT NULL,

    price DECIMAL(20,6) NOT NULL,
    method_data JSONB NOT NULL,

    calculation_version INTEGER NOT NULL DEFAULT 1,
    calculated_at TIMESTAMP NOT NULL DEFAULT NOW(),

    PRIMARY KEY (position_key, business_date, price_method),
    FOREIGN KEY (position_key, business_date)
        REFERENCES position_snapshots_settled(position_key, business_date)
);

-- Settlement date history (bitemporal)
CREATE TABLE position_snapshots_settled_history (
    history_id BIGSERIAL PRIMARY KEY,
    position_key VARCHAR(200) NOT NULL,
    business_date DATE NOT NULL,

    -- Metrics
    net_quantity BIGINT NOT NULL,
    gross_long BIGINT NOT NULL,
    gross_short BIGINT NOT NULL,
    trade_count INTEGER NOT NULL,
    total_notional DECIMAL(20,6),

    -- Bitemporal
    calculation_version INTEGER NOT NULL,
    calculated_at TIMESTAMP NOT NULL,
    superseded_at TIMESTAMP,

    -- Audit
    change_reason VARCHAR(50),
    previous_net_quantity BIGINT,
    calculation_request_id VARCHAR(50),

    -- Tracking
    last_sequence_num BIGINT NOT NULL,
    last_trade_time TIMESTAMP,
    calculation_method VARCHAR(20)
);

CREATE INDEX idx_settled_history_position_date ON position_snapshots_settled_history(position_key, business_date);
CREATE INDEX idx_settled_history_current ON position_snapshots_settled_history(position_key, business_date)
    WHERE superseded_at IS NULL;
