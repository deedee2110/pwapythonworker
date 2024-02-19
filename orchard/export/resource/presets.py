from datetime import datetime
from typing import List, Dict
import hashlib
from fastapi import APIRouter, HTTPException, status
from sqlalchemy.exc import SQLAlchemyError, DBAPIError

from orchard.export.model.exp_preset import ExpPreset, ExpPresetIn, ExpPresetManager

router = APIRouter()


@router.get("/", response_model=List[ExpPreset])
async def preset_list() -> List[ExpPreset]:
    """
    REturn a list of all ExpPreset.\f

    :return: a list of ExpPreset
    :rtype: List[ExpPreset]
    """
    man = ExpPresetManager()
    try:
        m_list: List[ExpPreset] = man.search("1")
    except (SQLAlchemyError, DBAPIError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"[presets] Failed t oget a list of presets: {e}"
        )
    return m_list


@router.post("/", response_model=Dict[str, str])
async def preset_add(dat_in: ExpPresetIn) -> dict:
    """
    Update a preset.\f
    """
    man = ExpPresetManager()
    try:
        m_preset_id=hashlib.sha1(('logbook_preset'+str(datetime.now())).encode()).hexdigest()
        # dat = ExpPreset(**dat_in.dict(),
        #                 preset_id=m_preset_id,
        #                 update_time=datetime.now(),
        #                 create_time=datetime.now())
        # print(dat)
        id = man.insert(dat_in)
    except (SQLAlchemyError, DBAPIError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"[presets] Failed to add new preset: {e}",
        )
    return {"preset_id": id}


@router.put("/{preset_id}", response_model=ExpPreset)
async def preset_update(preset_id: str, dat_in: ExpPresetIn):
    man = ExpPresetManager()
    try:
        org: ExpPreset = man.get_with_key(preset_id)
        # dat = ExpPreset(**dat_in.dict(),
        #                 preset_id=org.preset_id,
        #                 update_time=datetime.now(),
        #                 create_time=org.create_time)
        rows_updated = man.update(dat_in, preset_id)
        dat: ExpPreset = man.get_with_key(preset_id)
    except (SQLAlchemyError, DBAPIError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"[presets] Failed to update preset: {e}",
        )
    return dat


@router.get("/{preset_id}", response_model=ExpPreset)
async def preset_get(preset_id: str):
    man = ExpPresetManager()
    try:
        dat: ExpPreset = man.get_with_key(preset_id)
    except (SQLAlchemyError, DBAPIError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"[presets] Failed to update preset: {e}",
        )
    return dat


@router.delete("/{preset_id}", response_model=Dict[str, int])
async def preset_delete(preset_id: str) -> dict:
    man = ExpPresetManager()
    try:
        rows_deleted: int = man.delete(preset_id)
    except (SQLAlchemyError, DBAPIError) as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"[presets] Failed to delete template: {e}"
        )
    return {"rows_deleted": rows_deleted}
