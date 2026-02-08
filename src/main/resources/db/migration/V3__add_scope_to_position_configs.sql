ALTER TABLE position_configs
    ADD COLUMN scope VARCHAR(200) NOT NULL DEFAULT 'ALL';

ALTER TABLE position_configs
    ADD CONSTRAINT uq_config_type_key_format_scope UNIQUE (config_type, key_format, scope);
