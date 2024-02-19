from fastapi import APIRouter
from orchard.DocSmith.resource import templates

router = APIRouter()
router.include_router(templates.router, prefix="/templates")
