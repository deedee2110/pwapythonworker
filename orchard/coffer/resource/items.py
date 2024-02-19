import html
import json
import os
import magic
from typing import List, Dict, Optional
from fastapi import APIRouter, Header, UploadFile, File, Form, Response, status
from pydantic import BaseModel
from starlette.responses import StreamingResponse
from orchard.coffer.storage import StorageManager
from orchard.coffer.util import gen_access_token, validate_access_token
from datetime import datetime

router = APIRouter()


class TokenIn(BaseModel):
    key: str
    timeout_s: Optional[int] = 60


@router.post("/access_token")
async def access_token_post(token_in: TokenIn):
    """
    Service for generating token\f

    :param token_in:  key and time for generating token
    :return: token string (in json)
    """
    return {"token": gen_access_token(token_in.key, token_in.timeout_s)}


@router.get("/{path_name:path}")
async def object_list(path_name: str,
                      response: Response,
                      resp_type: Optional[str] = 'list',
                      token: Optional[str] = None) -> List[Dict]:
    """
    List / get object\f

    :param path_name: filename
    :param response:
    :param resp_type: list/data
    :param token: access token
    :return:
    """
    print("object_list")
    storage_man = StorageManager()
    if resp_type == 'list':
        res = storage_man.list_objects(path_name)
        arr = []
        for (item_obj, tags) in res:
            tags.pop('delete_key', None)
            tags.pop('view_key', None)
            tags.pop('update_key', None)
            arr.append({"object": item_obj, "tags": tags})
        return arr
    # return file content (data)
    (temp_file, obj, tags) = storage_man.get_file_object(path_name)
    valid = False
    if tags is None:
        valid = True
    else:
        if 'view_key' in tags.keys():
            if validate_access_token(tags['view_key'], token):
                valid = True
        else:
            valid = True

    if not valid:
        res = {"status": "failure",
               "operation": "coffer.get_object",
               "message": "invalid object/permission"}
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return res

    if obj is None:
        res = {"status": "failure",
               "operation": "coffer.get_object",
               "message": "invalid object"}
        response.status_code = status.HTTP_404_NOT_FOUND
        return res

    print("filename:", obj.object_name)
    print("content_type:", obj.content_type)
    # return FileResponse(temp_file, media_type=obj.content_type)
    fp = open(temp_file, mode="rb")
    mime = magic.Magic(mime=True)
    response = StreamingResponse(fp, media_type=mime.from_file(temp_file))
    # response.headers["Content-Disposition"] = "attachment; filename="+template.template_name+".pdf"
    response.headers["Content-Disposition"] = "filename*=UTF-8''" + \
                                            html.escape(os.path.basename(obj.object_name))
    return response


@router.post("/{path_name:path}")
async def object_post(path_name: str,
                      response: Response,
                      CofferToken: Optional[str] = Header(None),
                      file: Optional[List[UploadFile]] = File(default=None),
                      tags: Optional[str] = Form(default='{}')):
    """
    Create/Update object. To upload multiple files, end the path_name with '/' (eg. test/).
    The system will generate a timestamp and pad it with the filename.\f

    :param path_name: base filename
    :param response: 
    :param CofferToken: access token
    :param file: upload file
    :param tags: json (key/value) for tags
    :return:
    """
    storage_man = StorageManager()
    print('object_post')
    results = []
    for file_item in file:
        m_path_name = path_name
        print('path_name', m_path_name)
        if (path_name[-1] == '/'):
            now = datetime.now()
            date_format = "%Y%m%d_%H%M%S"
            m_path_name += now.strftime(date_format) + "_" + file_item.filename
        # check permission
        old_tags = storage_man.get_object_tags(m_path_name)
        valid = False
        if old_tags is None:
            valid = True
        else:
            if 'update_key' in old_tags.keys():
                if validate_access_token(old_tags['update_key'], CofferToken):
                    valid = True
            else:
                valid = True

        if not valid:
            res = {"status": "failure",
                   "operation": "coffer.object_post",
                   "message": "invalid object/permission"}
            response.status_code = status.HTTP_401_UNAUTHORIZED
            return res

        if old_tags is None:
            old_tags = {}

        # operations
        m_tags = {**old_tags, **json.loads(tags)}
        print('m_tags', m_tags)
        if file_item is not None:
            m_tags['filename'] = file_item.filename
            # mtags['content_type'] = file.content_type
            mime = magic.Magic(mime=True)
            file_item.file.seek(0)
            m_tags['content_type'] = mime.from_buffer(file_item.file.read(1024))
            file_item.file.seek(0)
            res = storage_man.put_object_stream(m_path_name, file_item.file, -1, m_tags)
        else:
            res = storage_man.set_object_tags(m_path_name, m_tags)
        results.append({"status": "success",
                        "object": res,
                        "tags": m_tags}
                       )
    return results


@router.delete("/{path_name:path}")
async def object_delete(path_name: str,
                        response: Response,
                        CofferToken: Optional[str] = Header(None)):
    """
    Delete object\f

    :param path_name: filename
    :param response:
    :param CofferToken: access token
    :return:
    """
    storage_man = StorageManager()
    tags = storage_man.get_object_tags(path_name)
    # check permission
    valid = False
    if tags is None:
        valid = True
    else:
        if 'delete_key' in tags.keys():
            if validate_access_token(tags['delete_key'], CofferToken):
                valid = True
        else:
            valid = True

    if not valid:
        res = {"status": "failure",
               "operation": "coffer.object_delete",
               "message": "invalid object/permission"}
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return res

    # delete operation
    res = storage_man.remove_object(path_name)
    return {"status": "success",
            "operation": "coffer.object_delete"}
