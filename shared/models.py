import uuid
from datetime import datetime

from sqlalchemy import String, Boolean, Integer, DECIMAL, Numeric, ForeignKey, DateTime, Date, Index, text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy.dialects.postgresql import UUID

from shared.database import Base


class Promoter(Base):
    __tablename__ = "promoters"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    email: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    contact_number: Mapped[str] = mapped_column(String(15), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    attendance: Mapped[list["Attendance"]] = relationship(
        back_populates="promoter", cascade="all, delete-orphan"
    )
    refresh_tokens: Mapped[list["RefreshToken"]] = relationship(
        back_populates="promoter", cascade="all, delete-orphan"
    )
    daily_sales: Mapped[list["DailySale"]] = relationship(
        back_populates="promoter", cascade="all, delete-orphan"
    )
    daily_stock_summaries: Mapped[list["DailyStockSummary"]] = relationship(
        back_populates="promoter", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_promoters_name", "name"),
        Index("idx_promoters_contact", "contact_number"),
    )


class Attendance(Base):
    __tablename__ = "attendance"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    promoter_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("promoters.id", ondelete="CASCADE"),
        nullable=False,
    )
    punch_in_timestamp: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    punch_in_lat: Mapped[float] = mapped_column(DECIMAL(9, 6), nullable=False)
    punch_in_lng: Mapped[float] = mapped_column(DECIMAL(9, 6), nullable=False)
    punch_in_store: Mapped[str] = mapped_column(String(255), nullable=False)
    punch_out_timestamp: Mapped[datetime | None] = mapped_column(
        DateTime, nullable=True
    )
    punch_out_lat: Mapped[float | None] = mapped_column(DECIMAL(9, 6), nullable=True)
    punch_out_lng: Mapped[float | None] = mapped_column(DECIMAL(9, 6), nullable=True)
    punch_out_store: Mapped[str | None] = mapped_column(String(255), nullable=True)

    promoter: Mapped["Promoter"] = relationship(back_populates="attendance")

    __table_args__ = (
        Index("idx_attendance_promoter", "promoter_id"),
        Index("idx_attendance_punch_in_time", "punch_in_timestamp"),
        Index("idx_attendance_punch_in_store", "punch_in_store"),
        Index("idx_attendance_promoter_date", "promoter_id", "punch_in_timestamp"),
        Index(
            "idx_attendance_active_sessions",
            "punch_out_timestamp",
            postgresql_where=text("punch_out_timestamp IS NULL"),
        ),
    )


class PasswordResetOTP(Base):
    __tablename__ = "password_reset_otps"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    otp_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_used: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_otp_email", "email"),
        Index("idx_otp_expires", "expires_at"),
    )


class Product(Base):
    __tablename__ = "products"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    sr_no: Mapped[int] = mapped_column(Integer, unique=True, nullable=False)
    mrp: Mapped[int] = mapped_column(Integer, nullable=False)
    ean: Mapped[str] = mapped_column(String(13), unique=True, nullable=False)
    article_code: Mapped[str] = mapped_column(String(15), unique=True, nullable=False)
    description: Mapped[str] = mapped_column(String(255), nullable=False)
    size_kg: Mapped[float] = mapped_column(Numeric(6, 3), nullable=False)
    gst_rate: Mapped[float] = mapped_column(Numeric(4, 2), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    __table_args__ = (
        Index("idx_products_mrp", "mrp"),
        Index("idx_products_size_mrp", "size_kg", "mrp"),
    )


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    promoter_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("promoters.id", ondelete="CASCADE"),
        nullable=False,
    )
    token: Mapped[str] = mapped_column(String(512), unique=True, nullable=False)
    expires_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    is_revoked: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    promoter: Mapped["Promoter"] = relationship(back_populates="refresh_tokens")

    __table_args__ = (
        Index("idx_refresh_promoter_active", "promoter_id", "is_revoked"),
        Index("idx_refresh_expires", "expires_at"),
    )


class DailySale(Base):
    __tablename__ = "daily_sales"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    attendance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("attendance.id", ondelete="CASCADE"),
        nullable=False,
    )
    promoter_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("promoters.id", ondelete="CASCADE"),
        nullable=False,
    )
    ean: Mapped[str] = mapped_column(String(13), nullable=False)
    qty_sold: Mapped[int] = mapped_column(Integer, nullable=False)
    sold_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    promoter: Mapped["Promoter"] = relationship(back_populates="daily_sales")
    attendance: Mapped["Attendance"] = relationship()

    __table_args__ = (
        Index("idx_daily_sales_promoter_date", "promoter_id", "sold_at"),
        Index("idx_daily_sales_attendance", "attendance_id"),
        Index("idx_daily_sales_ean", "ean"),
    )


class DailyStockSummary(Base):
    __tablename__ = "daily_stock_summary"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, default=uuid.uuid4
    )
    attendance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("attendance.id", ondelete="CASCADE"),
        nullable=False,
    )
    promoter_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("promoters.id", ondelete="CASCADE"),
        nullable=False,
    )
    ean: Mapped[str] = mapped_column(String(13), nullable=False)
    opening_qty: Mapped[int] = mapped_column(Integer, nullable=False)
    qty_received: Mapped[int] = mapped_column(Integer, nullable=False)
    qty_sold: Mapped[int] = mapped_column(Integer, nullable=False)
    closing_stock: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )

    promoter: Mapped["Promoter"] = relationship(back_populates="daily_stock_summaries")
    attendance: Mapped["Attendance"] = relationship()

    __table_args__ = (
        Index("idx_daily_stock_promoter_date", "promoter_id", "created_at"),
        Index("idx_daily_stock_attendance", "attendance_id"),
        Index("idx_daily_stock_ean", "ean"),
    )


class ColdStorageStock(Base):
    __tablename__ = "cold_storage_stocks"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    inward_dt: Mapped[datetime | None] = mapped_column(Date, nullable=True)
    unit: Mapped[str | None] = mapped_column(String(50), nullable=True)
    inward_no: Mapped[str | None] = mapped_column(String(50), nullable=True)
    item_mark: Mapped[str | None] = mapped_column(String(255), nullable=True)
    vakkal: Mapped[str | None] = mapped_column(String(255), nullable=True)
    lot_no: Mapped[str | None] = mapped_column(String(50), nullable=True)
    no_of_cartons: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    weight_kg: Mapped[float | None] = mapped_column(Numeric(12, 3), nullable=True)
    total_inventory_kgs: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    group_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    item_description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    storage_location: Mapped[str | None] = mapped_column(String(255), nullable=True)
    exporter: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_purchase_rate: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    value: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    boxes: Mapped[list["ColdStorageBox"]] = relationship(
        back_populates="stock", cascade="all, delete-orphan"
    )

    __table_args__ = (
        Index("idx_cs_group_name", "group_name"),
        Index("idx_cs_storage_location", "storage_location"),
        Index("idx_cs_exporter", "exporter"),
        Index("idx_cs_item_mark", "item_mark"),
        Index("idx_cs_inward_dt", "inward_dt"),
    )


class ColdStorageBox(Base):
    __tablename__ = "cold_storage_boxes"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    stock_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("cold_storage_stocks.id", ondelete="CASCADE"),
        nullable=False,
    )
    box_number: Mapped[int] = mapped_column(Integer, nullable=False)
    box_id: Mapped[str | None] = mapped_column(String(50), nullable=True)
    item_description: Mapped[str | None] = mapped_column(String(500), nullable=True)
    lot_no: Mapped[str | None] = mapped_column(String(50), nullable=True)
    weight_kg: Mapped[float | None] = mapped_column(Numeric(12, 3), nullable=True)
    status: Mapped[str] = mapped_column(String(50), default="available", nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    stock: Mapped["ColdStorageStock"] = relationship(back_populates="boxes")

    __table_args__ = (
        Index("idx_cs_boxes_stock_id", "stock_id"),
        Index("idx_cs_boxes_box_id", "box_id"),
        Index("idx_cs_boxes_lot_no", "lot_no"),
        Index("idx_cs_boxes_status", "status"),
    )
