-- Add new JSONB column
ALTER TABLE position_configs ADD COLUMN scope_json JSONB;

-- Convert existing data
UPDATE position_configs SET scope_json = '{"type":"ALL"}'::jsonb;

-- Swap columns
ALTER TABLE position_configs ALTER COLUMN scope_json SET NOT NULL;
ALTER TABLE position_configs ALTER COLUMN scope_json SET DEFAULT '{"type":"ALL"}'::jsonb;
ALTER TABLE position_configs DROP CONSTRAINT IF EXISTS uq_config_type_key_format_scope;
ALTER TABLE position_configs DROP COLUMN scope;
ALTER TABLE position_configs RENAME COLUMN scope_json TO scope;

-- Recreate unique constraint
ALTER TABLE position_configs
    ADD CONSTRAINT uq_config_type_key_format_scope UNIQUE (config_type, key_format, scope);
