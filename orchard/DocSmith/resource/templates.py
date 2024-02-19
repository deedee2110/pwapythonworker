from typing import Optional
from fastapi import APIRouter
from orchard.DocSmith.model.doc_template import DocTemplateManager, DocTemplateIn
from orchard.resource import BaseResource

router = APIRouter()


class TemplateResource(BaseResource):

    def __init__(self):
        super().__init__(DocTemplateManager)


res_man = TemplateResource()


@router.get("/")
async def template_list(search_query: Optional[str] = None,
                        page: int = 0,
                        page_size: int = 100,
                        order_by: Optional[str] = None,
                        format: Optional[str] = None):
    return res_man.get(search_query, page, page_size, order_by, format)


@router.post("/")
async def template_add(template_in: DocTemplateIn):
    if template_in.page_width is None:
        template_in.page_width = 0
    if template_in.page_height is None:
        template_in.page_height = 0
    return res_man.post(template_in)


@router.put("/{template_id}")
async def template_update(template_id: str, template_in: DocTemplateIn):
    if template_in.page_width is None:
        template_in.page_width = 0
    if template_in.page_height is None:
        template_in.page_height = 0
    return res_man.put_with_id(template_id, template_in)


@router.get("/{template_id}")
async def template_get(template_id: str):
    return res_man.get_with_id(template_id)


@router.delete("/{template_id}")
async def template_delete(template_id: str):
    return res_man.delete_with_id(template_id)
