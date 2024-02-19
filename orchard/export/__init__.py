
from fastapi import APIRouter
from orchard.export.resource import presets

router = APIRouter()
router.include_router(presets.router, prefix="/presets")