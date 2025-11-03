CREATE TABLE IF NOT EXISTS tickers (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS tg_news (
    id SERIAL PRIMARY KEY,
    Date TIMESTAMP NOT NULL,
    news TEXT NOT NULL,
    source TEXT,
    clean_news TEXT
);

CREATE TABLE task_logs (
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(250),
    task_id VARCHAR(250),
    log_level VARCHAR(50),
    message TEXT,
    timestamp TIMESTAMP,

    created_at TIMESTAMP DEFAULT NOW()
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

CREATE TABLE IF NOT EXISTS ohlcv_features (
    id SERIAL PRIMARY KEY,
    ticker TEXT NOT NULL,
    date TIMESTAMP NOT NULL,

    -- Скользящие средние
    sma_5 FLOAT,
    sma_20 FLOAT,
    sma_50 FLOAT,
    sma_200 FLOAT,

    ema_20 FLOAT,
    ema_50 FLOAT,

    -- Индикаторы тренда
    macd FLOAT,
    macd_signal FLOAT,
    macd_hist FLOAT,

    adx FLOAT,

    -- Индикаторы волатильности
    volatility FLOAT,
    atr FLOAT,

    -- Осцилляторы (momentum)
    rsi FLOAT,
    stoch_k FLOAT,
    stoch_d FLOAT,

    -- Полосы Боллинджера
    bb_upper FLOAT,
    bb_middle FLOAT,
    bb_lower FLOAT,
    bb_width FLOAT,

    -- Доходность
    daily_return FLOAT,
    cumulative_return FLOAT,

    processed_at TIMESTAMP NOT NULL DEFAULT NOW()
);
CREATE OR REPLACE VIEW v_ohlcv_full AS
SELECT
    o.id AS ohlcv_id,
    o.ticker,
    o.date,
    o.open,
    o.high,
    o.low,
    o.close,
    o.volume,
    f.sma_5,
    f.sma_20,
    f.volatility,
    f.daily_return,
    f.processed_at
FROM ohlcv o
LEFT JOIN ohlcv_features f
    ON o.ticker = f.ticker AND o.date = f.date
ORDER BY o.ticker, o.date;


INSERT INTO tickers (name) VALUES
    ('GAZP'),  
    ('SBER'),  
    ('LKOH'),  
    ('YNDX'),  
    ('TATN'),  
    ('ROSN')  
ON CONFLICT DO NOTHING;

INSERT INTO fetch_settings (ticker_id, interval, lookback_days)
SELECT id, '1d', 1825 FROM tickers
ON CONFLICT DO NOTHING;