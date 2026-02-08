CREATE TABLE position_configs (
    config_id BIGSERIAL PRIMARY KEY,
    config_type VARCHAR(20) NOT NULL,
    name VARCHAR(100) NOT NULL,
    key_format VARCHAR(50) NOT NULL,
    price_methods VARCHAR(200) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (name)
);

-- Seed the default OFFICIAL config (matches Phase 1 hardcoded config)
INSERT INTO position_configs (config_id, config_type, name, key_format, price_methods)
VALUES (1, 'OFFICIAL', 'Official Positions', 'BOOK_COUNTERPARTY_INSTRUMENT', 'WAC');
