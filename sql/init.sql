CREATE TABLE IF NOT EXISTS tickers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS fetch_settings (
    id SERIAL PRIMARY KEY,
    ticker_id INT REFERENCES tickers(id) ON DELETE CASCADE,
    interval TEXT DEFAULT '1d',       
    lookback_days INT DEFAULT 1825      
);

CREATE TABLE IF NOT EXISTS ohlcv (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL REFERENCES tickers(name) ON DELETE CASCADE,
    date TIMESTAMP NOT NULL,
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume FLOAT
);

CREATE TABLE IF NOT EXISTS predictions (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL REFERENCES tickers(name) ON DELETE CASCADE,
    date TIMESTAMP NOT NULL,
    model TEXT NOT NULL,
    prediction FLOAT
);

INSERT INTO tickers (name) VALUES
    ('GAZP.ME'),  
    ('SBER.ME'),  
    ('LKOH.ME'),  
    ('YNDX.ME'),  
    ('TATN.ME'),  
    ('ROSN.ME')  
ON CONFLICT DO NOTHING;

INSERT INTO fetch_settings (ticker_id, interval, lookback_days)
SELECT id, '1d', 1825 FROM tickers
ON CONFLICT DO NOTHING;