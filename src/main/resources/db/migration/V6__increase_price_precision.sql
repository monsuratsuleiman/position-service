-- Increase WAC average price precision from 6 to 12 decimal places
-- to prevent rounding accumulation during incremental calculations

ALTER TABLE position_average_prices
    ALTER COLUMN price TYPE DECIMAL(20, 12);

ALTER TABLE position_average_prices_settled
    ALTER COLUMN price TYPE DECIMAL(20, 12);
