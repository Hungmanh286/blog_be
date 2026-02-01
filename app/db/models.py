"""
Database Models
All SQLAlchemy/SQLModel models are defined here.
"""

from datetime import datetime

from sqlalchemy import Column, Integer, DateTime, String, Boolean
from sqlalchemy.ext.declarative import as_declarative, declared_attr


@as_declarative()
class Base:
    __abstract__ = True
    __name__: str

    # Generate __tablename__ automatically
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()


class BareBaseModel(Base):
    __abstract__ = True

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)


class User(BareBaseModel):
    """User model for authentication and authorization."""

    full_name = Column(String, index=True)
    email = Column(String, unique=True, index=True)
    hashed_password = Column(String(255))
    is_active = Column(Boolean, default=True)
    role = Column(String, default="guest")
    last_login = Column(DateTime)


# =============================================================================
# Market Data Models
# =============================================================================

from sqlalchemy import Date, Float


class MarketIndicators(Base):
    """Dữ liệu thị trường hàng ngày (Market Data)."""

    __tablename__ = "market_indicators"

    report_date = Column(Date, primary_key=True)  # date (ví dụ 1/9/2026)
    volatility_index = Column(Float)  # vol
    breadth_index = Column(Float)  # bre
    market_price = Column(Integer)  # map
    dividend_12m_pct = Column(Float)  # div (%dividen_12M)

    # Nhóm EPS
    eps_vnindex = Column(Float)  # eps
    eps_nonbank = Column(Float)  # eps

    # Nhóm Short Term (%st)
    st_pe_under_10_pct = Column(Float)  # %pe<10
    st_pb_under_1_pct = Column(Float)  # %pb<1

    # Nhóm PB
    pb_vnindex = Column(Float)  # pb
    pb_nonbank = Column(Float)  # pb
    pb_bank = Column(Float)  # pb

    # Nhóm Return (ret)
    ret_40years_old_pct = Column(Float)  # ret (ví dụ 99.1)
    ret_vni_adjusted_pct = Column(Float)  # ret

    turnover_ratio = Column(Float)  # tur
    derivatives_ratio = Column(Float)  # der
    insider_transaction_3m_pct = Column(Float)  # ins
    probability = Column(Float)  # tra
    avg_50d_orders = Column(Integer)  # avg
    matching_rate_pct = Column(Float)  # mat (ví dụ 53.3)

    # Nhóm Correlation (cor)
    cor_spx = Column(Float)
    cor_vn1y = Column(Float)
    cor_usd = Column(Float)

    # Nhóm Health (hea)
    hea_consumption = Column(Float)
    hea_production = Column(Float)
    hea_labor = Column(Float)


class PortfolioPerformance(Base):
    """Hiệu suất danh mục theo năm (Portfolio Performance)."""

    __tablename__ = "portfolio_performance"

    year = Column(Integer, primary_key=True)  # por(year)
    por_40_years_old = Column(Float)  # por_40_years_old
    por_vni_adjusted = Column(Float)  # por_vni_adjusted


class SectorValuation(Base):
    """Định giá theo ngành (Sector Valuation)."""

    __tablename__ = "sector_valuation"

    sector_name = Column(String(100), primary_key=True)  # sec (ngành)
    pe_percentile = Column(Float)  # pe_percentile
    pb_percentile = Column(Float)  # pb_percentile


class WorldMarketAnalysis(Base):
    """Phân tích thị trường thế giới (World Market Analysis - WMA)."""

    __tablename__ = "world_market_analysis"

    sector = Column(String(100), primary_key=True)  # Ngành
    pe_percentile = Column(Float)  # PE percentile
    pb_percentile = Column(Float)  # PB percentile


class MacroIndicators(Base):
    """Chỉ số vĩ mô theo tháng (Macro Monthly)."""

    __tablename__ = "macro_indicators"

    month_label = Column(String(20), primary_key=True)  # month (ví dụ "thg 1-23")
    gdp_growth_pct = Column(Float)  # %gdp(%gdb)
    m2_growth_pct = Column(Float)  # %gdp(%m2)
    mar_margin = Column(Float)  # mar(mar_margin)
    mar_deposit = Column(Float)  # mar(mar_deposit)
