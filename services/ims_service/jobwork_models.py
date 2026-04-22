"""
Jobwork module — SQLAlchemy ORM models
Maps to actual jb_* tables created by job_work_server._ensure_tables()

Tables:
  jb_materialout_header  — dispatch/order header
  jb_materialout_lines   — dispatched line items
  jb_work_inward_receipt — inward receipt header
  jb_work_inward_lines   — inward receipt line items (FG, waste, rejection)
"""

from sqlalchemy import (
    Column, Integer, String, Numeric, Boolean, Text, DateTime, ForeignKey, func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship

from shared.database import Base


class JbMaterialoutHeader(Base):
    __tablename__ = "jb_materialout_header"

    id = Column(Integer, primary_key=True, autoincrement=True)
    challan_no = Column(String(100), nullable=False)
    job_work_date = Column(String(20))
    from_warehouse = Column(String(100))
    to_party = Column(String(255))
    party_address = Column(Text)
    party_state = Column(String(100))
    party_city = Column(String(100))
    party_pin_code = Column(String(10))
    party_contact_company = Column(String(255))
    party_contact_mobile = Column(String(50))
    party_email = Column(String(255))
    sub_category = Column(String(100))
    contact_person = Column(String(255))
    contact_number = Column(String(50))
    purpose_of_work = Column(Text)
    expected_return_date = Column(String(20))
    vehicle_no = Column(String(50))
    driver_name = Column(String(100))
    authorized_person = Column(String(255))
    remarks = Column(Text)
    e_way_bill_no = Column(String(100))
    dispatched_through = Column(String(255))
    type = Column(String(10), nullable=False, default="OUT")
    status = Column(String(30), nullable=False, default="sent")
    dispatch_to = Column(JSONB)
    payload = Column(JSONB)
    created_by = Column(String(255))
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    lines = relationship("JbMaterialoutLine", back_populates="header", lazy="select")
    inward_receipts = relationship("JbWorkInwardReceipt", back_populates="header", lazy="select")


class JbMaterialoutLine(Base):
    __tablename__ = "jb_materialout_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    header_id = Column(Integer, ForeignKey("jb_materialout_header.id", ondelete="CASCADE"))
    sl_no = Column(Integer)
    item_description = Column(String(500))
    material_type = Column(String(50))
    item_category = Column(String(100))
    sub_category = Column(String(100))
    quantity_kgs = Column(Numeric(12, 3), default=0)
    quantity_boxes = Column(Integer, default=0)
    rate_per_kg = Column(Numeric(12, 2), default=0)
    amount = Column(Numeric(12, 2), default=0)
    uom = Column(String(20))
    case_pack = Column(String(20))
    net_weight = Column(String(20))
    total_weight = Column(String(20))
    batch_number = Column(String(100))
    lot_number = Column(String(100))
    manufacturing_date = Column(String(20))
    expiry_date = Column(String(20))
    line_remarks = Column(Text)
    cold_unit = Column(String(50))
    item_mark = Column(String(255))
    box_id = Column(String(100))
    transaction_no = Column(String(100))
    cold_storage_snapshot = Column(JSONB)

    header = relationship("JbMaterialoutHeader", back_populates="lines")


class JbWorkInwardReceipt(Base):
    __tablename__ = "jb_work_inward_receipt"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ir_number = Column(String(100), nullable=False)
    challan_no = Column(String(100))
    header_id = Column(Integer, ForeignKey("jb_materialout_header.id", ondelete="CASCADE"))
    receipt_date = Column(String(20))
    receipt_type = Column(String(10), nullable=False, default="partial")
    vehicle_no = Column(String(50))
    driver_name = Column(String(100))
    inward_warehouse = Column(String(255))
    remarks = Column(Text)
    created_by = Column(String(255))
    created_at = Column(DateTime, default=func.now())

    header = relationship("JbMaterialoutHeader", back_populates="inward_receipts")
    lines = relationship("JbWorkInwardLine", back_populates="receipt", lazy="select")


class JbWorkInwardLine(Base):
    __tablename__ = "jb_work_inward_lines"

    id = Column(Integer, primary_key=True, autoincrement=True)
    inward_receipt_id = Column(Integer, ForeignKey("jb_work_inward_receipt.id", ondelete="CASCADE"))
    sl_no = Column(Integer)
    item_description = Column(String(500))
    sent_kgs = Column(Numeric(12, 3), default=0)
    sent_boxes = Column(Integer, default=0)
    finished_goods_kgs = Column(Numeric(12, 3), default=0)
    finished_goods_boxes = Column(Integer, default=0)
    waste_kgs = Column(Numeric(12, 3), default=0)
    waste_type = Column(String(100))
    rejection_kgs = Column(Numeric(12, 3), default=0)
    rejection_boxes = Column(Integer, default=0)
    line_remarks = Column(Text)
    process_type = Column(String(100))
    min_loss_pct = Column(Numeric(5, 2), default=0)
    max_loss_pct = Column(Numeric(5, 2), default=0)
    waste_with_partial = Column(Boolean, default=True)
    single_shot = Column(Boolean, default=False)

    receipt = relationship("JbWorkInwardReceipt", back_populates="lines")
