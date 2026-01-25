-- =============================================================================
-- Database Initialization Script
-- Tự động chạy khi khởi động Docker PostgreSQL
-- =============================================================================

-- --- BẢNG 1: Dữ liệu thị trường hàng ngày (Market Data) ---
CREATE TABLE IF NOT EXISTS market_indicators (
    report_date DATE,                       -- date (ví dụ 1/9/2026)
    volatility_index FLOAT,                 -- vol
    breadth_index FLOAT,                    -- bre
    market_price INT,                       -- map
    dividend_12m_pct FLOAT,                 -- div (%dividen_12M)
    
    -- Nhóm EPS
    eps_vnindex FLOAT,                      -- eps
    eps_nonbank FLOAT,                      -- eps
    
    -- Nhóm Short Term (%st)
    st_pe_under_10_pct FLOAT,               -- %pe<10
    st_pb_under_1_pct FLOAT,                -- %pb<1
    
    -- Nhóm PB
    pb_vnindex FLOAT,                       -- pb
    pb_nonbank FLOAT,                       -- pb
    pb_bank FLOAT,                          -- pb
    
    -- Nhóm Return (ret)
    ret_40years_old_pct FLOAT,              -- ret (ví dụ 99.1)
    ret_vni_adjusted_pct FLOAT,             -- ret
    
    turnover_ratio FLOAT,                   -- tur
    derivatives_ratio FLOAT,                -- der
    insider_transaction_3m_pct FLOAT,       -- ins
    probability FLOAT,                      -- tra
    avg_50d_orders INT,                     -- avg
    matching_rate_pct FLOAT,                -- mat (ví dụ 53.3)
    
    cor_spx FLOAT,                          
    cor_vn1y FLOAT,
    cor_usd FLOAT,                      
    
    hea_consumption FLOAT,
    hea_production FLOAT,
    hea_labor FLOAT,

    PRIMARY KEY (report_date)
);

CREATE TABLE IF NOT EXISTS portfolio_performance (
    year INT,                               -- por(year)
    por_40_years_old FLOAT,                 -- por_40_years_old
    por_vni_adjusted FLOAT,                 -- por_vni_adjusted
    
    PRIMARY KEY (year)
);

CREATE TABLE IF NOT EXISTS sector_valuation (
    sector_name VARCHAR(100),               -- sec (ngành) - Dùng VARCHAR cho tương thích PostgreSQL
    pe_percentile FLOAT,                    -- pe_percentile
    pb_percentile FLOAT,                    -- pb_percentile
    
    PRIMARY KEY (sector_name)
);

-- --- BẢNG 4: Phân tích thị trường thế giới (World Market Analysis - WMA) ---
CREATE TABLE IF NOT EXISTS world_market_analysis (
    country VARCHAR(100),                   -- wma (country)
    ytd_pct FLOAT,                          -- ytd(%)
    percentile_growth FLOAT,                -- percentile growth
    percentile_valuation FLOAT,             -- percentile valuation
    
    PRIMARY KEY (country)
);

-- --- BẢNG 5: Chỉ số vĩ mô theo tháng (Macro Monthly) ---
CREATE TABLE IF NOT EXISTS macro_indicators (
    month_label VARCHAR(20),                -- month (ví dụ "thg 1-23")
    gdp_growth_pct FLOAT,                   -- %gdp(%gdb)
    m2_growth_pct FLOAT,                    -- %gdp(%m2)
    mar_margin FLOAT,                       -- mar(mar_margin)
    mar_deposit FLOAT,                      -- mar(mar_deposit)

    PRIMARY KEY (month_label) 
    -- Lưu ý: Tốt nhất nên lưu tháng dưới dạng DATE (ví dụ 2023-01-01) để dễ sắp xếp, 
    -- nhưng ở đây để VARCHAR theo yêu cầu.
);

-- =============================================================================
-- Indexes for performance optimization
-- =============================================================================

-- Index cho market_indicators
CREATE INDEX IF NOT EXISTS idx_market_indicators_date ON market_indicators(report_date);

-- Index cho portfolio_performance
CREATE INDEX IF NOT EXISTS idx_portfolio_performance_year ON portfolio_performance(year);

-- Index cho sector_valuation
CREATE INDEX IF NOT EXISTS idx_sector_valuation_name ON sector_valuation(sector_name);

-- Index cho world_market_analysis
CREATE INDEX IF NOT EXISTS idx_world_market_analysis_country ON world_market_analysis(country);

-- Index cho macro_indicators
CREATE INDEX IF NOT EXISTS idx_macro_indicators_month ON macro_indicators(month_label);

-- =============================================================================
-- Grant permissions (optional - adjust as needed)
-- =============================================================================
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
