CREATE TABLE IF NOT EXISTS ohlcv (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT
);

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    date TIMESTAMP NOT NULL,
    model TEXT NOT NULL,
    prediction FLOAT
);