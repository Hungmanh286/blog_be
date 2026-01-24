-- =============================================================================
-- Database Initialization Script
-- Tự động chạy khi khởi động Docker PostgreSQL
-- =============================================================================

-- 1. Bảng dữ liệu thị trường hàng ngày (Tất cả chỉ số biến động)
CREATE TABLE IF NOT EXISTS fact_market_daily (
    trade_date DATE PRIMARY KEY,
    -- Market Map
    price_close DECIMAL(15,2),
    -- Avg & Liquidity
    avg_50d_orders INT,
    turnover_ratio DECIMAL(10,4),
    matching_rate DECIMAL(10,2),
    -- Technicals
    volatility_index DECIMAL(10,2),
    breadth_index DECIMAL(10,2),
    trading_probability DECIMAL(10,2),
    derivative_ratio DECIMAL(10,2),
    insider_trans_3m DECIMAL(10,4),
    -- Valuation & Fundamentals
    dividend_yield_12m DECIMAL(10,2),
    pb_vnindex DECIMAL(10,2),
    pb_non_bank DECIMAL(10,2),
    pb_bank DECIMAL(10,2),
    eps_vnindex DECIMAL(10,2),
    eps_non_bank DECIMAL(10,2),
    -- Correlations & Economy
    corr_spx DECIMAL(10,4),
    corr_vn1y DECIMAL(10,4),
    corr_usd DECIMAL(10,4),
    econ_consumption DECIMAL(10,2),
    econ_production DECIMAL(10,2),
    econ_labor DECIMAL(10,2)
);

-- 2. Bảng dữ liệu Vĩ mô (Tháng/Quý)
CREATE TABLE IF NOT EXISTS fact_macro_periodic (
    report_date DATE PRIMARY KEY,
    gdp_cap_ratio DECIMAL(10,2),
    m2_growth DECIMAL(10,2),
    margin_ratio DECIMAL(10,2),
    deposit_ratio DECIMAL(10,2)
);

-- 3. Bảng Snapshot so sánh (Ngành & Thế giới)
CREATE TABLE IF NOT EXISTS dim_market_snapshot (
    entity_name VARCHAR(100) NOT NULL,
    category VARCHAR(50) NOT NULL, -- 'SECTOR' or 'GLOBAL'
    ytd_return DECIMAL(10,2),
    growth_score DECIMAL(5,2),      -- Percentile Growth
    valuation_score DECIMAL(5,2),   -- Percentile Valuation (chung)
    pe_percentile DECIMAL(5,2),     -- Riêng cho Sector
    pb_percentile DECIMAL(5,2),     -- Riêng cho Sector
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (entity_name, category)
);

-- 4.1 Bảng Thông tin danh mục
CREATE TABLE IF NOT EXISTS portfolio_info (
    portfolio_code VARCHAR(50) PRIMARY KEY, -- '40YO', 'VNI_ADJ'
    portfolio_name VARCHAR(100),
    return_since_inception DECIMAL(10,2),
    avg_return_1y DECIMAL(10,2),
    var_10 DECIMAL(10,2),
    alpha DECIMAL(10,2),
    beta DECIMAL(10,2),
    risk_free_rate DECIMAL(10,2),
    ret_2023 DECIMAL(10,2),
    ret_2024 DECIMAL(10,2),
    ret_2025 DECIMAL(10,2),
    ret_2026 DECIMAL(10,2)
);

-- 4.2 Bảng Lịch sử lợi nhuận danh mục
CREATE TABLE IF NOT EXISTS portfolio_daily_log (
    log_date DATE,
    portfolio_code VARCHAR(50),
    daily_return DECIMAL(10,5),
    PRIMARY KEY (log_date, portfolio_code),
    FOREIGN KEY (portfolio_code) REFERENCES portfolio_info(portfolio_code)
);

-- =============================================================================
-- Indexes for performance optimization
-- =============================================================================

-- Index cho fact_market_daily
CREATE INDEX IF NOT EXISTS idx_fact_market_daily_date ON fact_market_daily(trade_date);

-- Index cho fact_macro_periodic
CREATE INDEX IF NOT EXISTS idx_fact_macro_periodic_date ON fact_macro_periodic(report_date);

-- Index cho dim_market_snapshot
CREATE INDEX IF NOT EXISTS idx_dim_market_snapshot_category ON dim_market_snapshot(category);

-- Index cho portfolio_daily_log
CREATE INDEX IF NOT EXISTS idx_portfolio_daily_log_code ON portfolio_daily_log(portfolio_code);
CREATE INDEX IF NOT EXISTS idx_portfolio_daily_log_date ON portfolio_daily_log(log_date);

-- =============================================================================
-- Grant permissions (optional - adjust as needed)
-- =============================================================================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
