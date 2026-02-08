-- Make dimension columns nullable
ALTER TABLE position_keys ALTER COLUMN book DROP NOT NULL;
ALTER TABLE position_keys ALTER COLUMN counterparty DROP NOT NULL;
ALTER TABLE position_keys ALTER COLUMN instrument DROP NOT NULL;

-- Null out irrelevant dimensions for existing rows based on key_format
UPDATE position_keys pk
SET book = NULL
FROM position_configs pc
WHERE pk.config_id = pc.config_id
  AND pc.key_format IN ('COUNTERPARTY_INSTRUMENT', 'INSTRUMENT');

UPDATE position_keys pk
SET counterparty = NULL
FROM position_configs pc
WHERE pk.config_id = pc.config_id
  AND pc.key_format IN ('BOOK_INSTRUMENT', 'INSTRUMENT', 'BOOK');

UPDATE position_keys pk
SET instrument = NULL
FROM position_configs pc
WHERE pk.config_id = pc.config_id
  AND pc.key_format = 'BOOK';
