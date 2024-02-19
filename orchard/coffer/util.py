import base64
import datetime
import json

from Crypto.Cipher import AES
from Crypto import Random

from orchard import settings

AES_KEY = settings.config.AES_KEY
AES_IV = settings.config.AES_IV


def gen_access_token(key: str, expire_in_s: int = 60) -> str:
    """
    Generate Access Token for item
    :param key: key
    :type key: str
    :param expire_in_s: age in seconds
    :type expire_in_s: int
    :return:
    """
    cipher = AES.new(AES_KEY.encode(), AES.MODE_CFB, AES_IV.encode())
    salt = Random.get_random_bytes(8).hex() + 'MyRandomSalt'
    exp_date = datetime.datetime.now() + datetime.timedelta(seconds=expire_in_s)
    obj = {"key": key,
           "expire": exp_date.isoformat()}
    json_str = salt + ':::::' + json.dumps(obj)
    token = cipher.encrypt(json_str.encode())
    return base64.b64encode(token)


def validate_access_token(org_key: str, token: str) -> bool:
    """
    Validate access token
    :param org_key: original key
    :type org_key: str
    :param token: token (generated from gen_access_token)
    :type token: str
    :return: valid
    :rtype: bool
    """
    if org_key is None:
        return True
    if token is None:
        return False
    cipher = AES.new(AES_KEY.encode(), AES.MODE_CFB, AES_IV.encode())
    temp_str = cipher.decrypt(base64.b64decode(token)).decode()
    json_str = temp_str.split(':::::', 2)[1]
    obj = json.loads(json_str)
    key = obj['key']
    exp_date = datetime.datetime.fromisoformat(obj['expire'])
    now = datetime.datetime.now()
    valid = True
    valid &= exp_date > now
    valid &= org_key.encode() == key.encode()
    # print('key',key,org_key)
    # print('expire',exp_date,now)
    return valid
