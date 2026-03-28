from typing import List

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from shared.database import get_db
from services.qc_service.models import (
    ApproverLoginRequest,
    ApproverLoginResponse,
    ApproverCreateRequest,
    ApproverResponse,
    DropdownResponse,
    FloorCreateRequest,
    FloorUpdateRequest,
    FloorResponse,
    FloorDeleteResponse,
    FactoryCreateRequest,
    FactoryUpdateRequest,
    FactoryResponse,
    FactoryDeleteResponse,
)
from services.qc_service.tools import (
    login_approver,
    create_approver,
    list_approvers,
    get_factories_floors,
    create_floor,
    update_floor,
    delete_floor,
    create_factory,
    update_factory,
    delete_factory,
)

router = APIRouter(prefix="/qc", tags=["qc"])


# ── Approver auth ─────────────────────────────


@router.post("/approver/login", response_model=ApproverLoginResponse)
def approver_login_endpoint(data: ApproverLoginRequest, db: Session = Depends(get_db)):
    return login_approver(data, db)


@router.post("/approver", response_model=ApproverResponse, status_code=201)
def create_approver_endpoint(data: ApproverCreateRequest, db: Session = Depends(get_db)):
    return create_approver(data, db)


@router.get("/approvers", response_model=List[ApproverResponse])
def list_approvers_endpoint(db: Session = Depends(get_db)):
    return list_approvers(db)


# ── Factory / Floor dropdown ──────────────────


@router.get("/dropdown/factories-floors", response_model=DropdownResponse)
def dropdown_endpoint(db: Session = Depends(get_db)):
    return get_factories_floors(db)


# ── Floor CRUD ────────────────────────────────


@router.post("/floors", response_model=FloorResponse, status_code=201)
def create_floor_endpoint(data: FloorCreateRequest, db: Session = Depends(get_db)):
    return create_floor(data, db)


@router.put("/floors/{floor_id}", response_model=FloorResponse)
def update_floor_endpoint(floor_id: int, data: FloorUpdateRequest, db: Session = Depends(get_db)):
    return update_floor(floor_id, data.model_dump(exclude_none=True), db)


@router.delete("/floors/{floor_id}", response_model=FloorDeleteResponse)
def delete_floor_endpoint(floor_id: int, db: Session = Depends(get_db)):
    return delete_floor(floor_id, db)


# ── Factory CRUD ──────────────────────────────


@router.post("/factories", response_model=FactoryResponse, status_code=201)
def create_factory_endpoint(data: FactoryCreateRequest, db: Session = Depends(get_db)):
    return create_factory(data, db)


@router.put("/factories/{factory_id}", response_model=FactoryResponse)
def update_factory_endpoint(factory_id: int, data: FactoryUpdateRequest, db: Session = Depends(get_db)):
    return update_factory(factory_id, data.model_dump(exclude_none=True), db)


@router.delete("/factories/{factory_id}", response_model=FactoryDeleteResponse)
def delete_factory_endpoint(factory_id: int, db: Session = Depends(get_db)):
    return delete_factory(factory_id, db)
