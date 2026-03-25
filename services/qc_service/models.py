from typing import List, Optional

from pydantic import BaseModel


# ── Approver ─────────────────────────────────


class ApproverLoginRequest(BaseModel):
    username: str
    password: str


class ApproverLoginResponse(BaseModel):
    success: bool
    approver_id: int
    username: str
    display_name: str


class ApproverCreateRequest(BaseModel):
    username: str
    password: str
    display_name: str


class ApproverResponse(BaseModel):
    id: int
    username: str
    display_name: str
    is_active: bool


# ── Factory / Floor ──────────────────────────


class FloorResponse(BaseModel):
    id: int
    floor_name: str
    sort_order: int


class FactoryWithFloors(BaseModel):
    id: int
    factory_code: str
    factory_name: Optional[str] = None
    floors: List[FloorResponse] = []


class DropdownResponse(BaseModel):
    factories: List[FactoryWithFloors] = []


class FloorCreateRequest(BaseModel):
    factory_code: str
    floor_name: str
    sort_order: Optional[int] = 0


class FloorUpdateRequest(BaseModel):
    floor_name: Optional[str] = None
    sort_order: Optional[int] = None


class FloorDeleteResponse(BaseModel):
    success: bool
    message: str


# ── Factory ──────────────────────────────────


class FactoryCreateRequest(BaseModel):
    factory_code: str
    factory_name: Optional[str] = None


class FactoryUpdateRequest(BaseModel):
    factory_code: Optional[str] = None
    factory_name: Optional[str] = None


class FactoryResponse(BaseModel):
    id: int
    factory_code: str
    factory_name: Optional[str] = None


class FactoryDeleteResponse(BaseModel):
    success: bool
    message: str
