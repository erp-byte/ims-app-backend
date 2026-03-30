"""
Jobwork module — SQLAlchemy ORM models + Pydantic schemas
Tables: jobwork_orders, jobwork_inward_receipts
"""

from datetime import date, datetime
from typing import Optional, List

from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime, Text,
    ForeignKey, func,
)
from sqlalchemy.orm import relationship

from shared.database import Base


# ── ORM Models ────────────────────────────────────────────────────

class JobworkOrder(Base):
    __tablename__ = "jobwork_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    jwo_id = Column(String(50), unique=True, nullable=False, index=True)
    company = Column(String(20), nullable=False, index=True)
    dispatch_date = Column(Date, nullable=False, index=True)
    vendor_name = Column(String(200), nullable=False, index=True)
    item_name = Column(String(200), nullable=False, index=True)
    item_description = Column(Text)
    process_type = Column(String(50), nullable=False, index=True)
    qty_dispatched = Column(Float, nullable=False, default=0)
    uom = Column(String(20), default="Kgs")
    jwo_status = Column(String(30), nullable=False, default="Open", index=True)
    expected_loss_pct = Column(Float, default=0)
    overdue_threshold_days = Column(Integer, default=30)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    inward_receipts = relationship("JobworkInwardReceipt", back_populates="jwo", lazy="select")


class JobworkInwardReceipt(Base):
    __tablename__ = "jobwork_inward_receipts"

    id = Column(Integer, primary_key=True, autoincrement=True)
    jwo_order_id = Column(Integer, ForeignKey("jobwork_orders.id"), nullable=False, index=True)
    ir_number = Column(String(50), unique=True, nullable=False)
    ir_date = Column(Date, nullable=False)
    receipt_type = Column(String(20), nullable=False, default="Partial")
    fg_qty_received = Column(Float, default=0)
    waste_qty_received = Column(Float, default=0)
    rejection_qty = Column(Float, default=0)
    actual_loss_pct = Column(Float, default=0)
    loss_status = Column(String(30), default="Pending")
    remarks = Column(Text)
    created_at = Column(DateTime, default=func.now())

    jwo = relationship("JobworkOrder", back_populates="inward_receipts")
