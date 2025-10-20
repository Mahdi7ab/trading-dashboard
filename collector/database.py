# collector/database.py

from sqlalchemy import (
    create_engine,
    Column,
    Integer,  # برای ستون id
    String,
    Float,
    BigInteger,
    Boolean
)
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URL

engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class Fill(Base):
    __tablename__ = "fills"

    # راه حل نهایی: یک id عددی و خودکار به عنوان کلید اصلی 🔑
    id = Column(Integer, primary_key=True, index=True)

    # بقیه ستون‌ها به عنوان داده‌های معمولی ذخیره می‌شوند
    hash = Column(String, index=True)
    oid = Column(BigInteger, index=True)
    user_address = Column(String, index=True)
    asset = Column(String, index=True)
    price = Column(Float)
    size = Column(Float)
    is_buy = Column(Boolean)
    direction = Column(String)
    pnl = Column(Float, nullable=True)
    timestamp = Column(BigInteger, index=True)

class TrackedTrader(Base):
    """
    جدولی برای نگهداری لیست تریدرهای سودده که باید دنبال شوند.
    این جدول توسط discover_traders.py پر می‌شود.
    """
    __tablename__ = "tracked_traders"

    id = Column(Integer, primary_key=True, index=True)
    user_address = Column(String, unique=True, index=True, nullable=False)
    pnl = Column(Float, nullable=True) # برای ذخیره سود تریدر در زمان کشف شدن

    def __repr__(self):
        return f"<TrackedTrader(user_address='{self.user_address}', pnl={self.pnl})>"