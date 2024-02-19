import hashlib
import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, UploadFile, Response, status
from fastapi.responses import FileResponse


router = APIRouter()


def gen_abs_target(tag, filename):
    abs_target = f'/data/{tag[0:2]}/{tag}/{filename}'
    return abs_target


def upload_to_file(file: UploadFile, abs_target: str) -> Path:
    dirs = os.path.dirname(abs_target)
    os.makedirs(dirs, exist_ok=True)
    try:
        with open(abs_target, "wb") as target_file:
            shutil.copyfileobj(file.file, target_file)
    finally:
        file.file.close()
    print(datetime.now, '[Storage] - save file to ', abs_target);
    return Path(abs_target)


def read_meta(tag, filename):
    meta_file = gen_abs_target(tag, filename) + '.meta'
    if not os.path.exists(meta_file):
        print(f'cannot find meta_file {meta_file}')
        return False
    meta = {}
    with open(meta_file) as m:
        content = m.read()
    try:
        meta = json.loads(content)
    except:
        print("Parsing json error ", meta)
    return meta


@router.post('/{tag}/{filename}')
async def post_file(
        response: Response,
        tag: str,
        filename: str,
        file: UploadFile):
    # existing file will be replaced
    try:
        target_file = gen_abs_target(tag, filename)
        upload_to_file(file, target_file)
        salt = 'KPR' + str(datetime.now())
        access_key = hashlib.sha1((salt + target_file).encode('utf8')).hexdigest()
        meta = {
            'filename': file.filename,
            'content_type': file.content_type,
            'access_key': access_key
        }
        with open(target_file + '.meta', "w") as m:
            m.write(json.dumps(meta))
    except:
        response.status_code = status.HTTP_409_CONFLICT
    return meta


@router.get('/{tag}/{filename}')
async def get_file(
        response: Response,
        tag: str,
        filename: str,
        key: Optional[str] = '',
        download: bool = False
        ):
    meta = read_meta(tag, filename)
    if meta is False:
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    if 'access_key' not in meta.keys() or meta['access_key'] != key:
        print(f'invalid access_key {key}')
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return
    abs_target = gen_abs_target(tag, filename)
    if not os.path.exists(abs_target):
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    if download:
        return FileResponse(abs_target, filename=meta['filename'], media_type=meta['content_type'])
    else:
        return FileResponse(abs_target, media_type=meta['content_type'])


@router.get('/{tag}/{filename}/meta')
async def get_meta(
        response: Response,
        tag: str,
        filename: str,
        key: Optional[str] = ''
        ):
    # is_me = tag == 'me'
    # if tag == 'me':
    #     tag = active_user.student_id
    meta = read_meta(tag, filename)
    if meta is False:
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    if 'access_key' not in meta.keys() or meta['access_key'] != key:
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return
    return meta


@router.delete('/{tag}/{filename}')
async def del_file(
        response: Response,
        tag: str,
        filename: str,
        key: Optional[str] = ''
        ):
    # if tag == 'me':
    #     tag = active_user.student_id
    meta = read_meta(tag, filename)
    if meta is False:
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    if 'access_key' not in meta.keys() or meta['access_key'] != key:
        response.status_code = status.HTTP_401_UNAUTHORIZED
        return
    abs_target = gen_abs_target(tag, filename)
    if not os.path.exists(abs_target):
        response.status_code = status.HTTP_404_NOT_FOUND
        return
    try:
        os.unlink(abs_target)
        os.unlink(abs_target + '.meta')
    except:
        response.status_code = status.HTTP_403_FORBIDDEN
    return
