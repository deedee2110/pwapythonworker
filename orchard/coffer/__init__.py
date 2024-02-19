from fastapi import APIRouter
from orchard.coffer.resource import items

router = APIRouter()
router.include_router(items.router, prefix="")
