# Base class for stroage subsystem based on minio (S3-compatible service)
import io
import logging
from tempfile import NamedTemporaryFile

import urllib3.exceptions
from minio import Minio, S3Error

# logging
from minio.commonconfig import Tags

from orchard import settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MINIO_ENDPOINT = settings.config.MINIO_ENDPOINT
MINIO_ACCESS_KEY = settings.config.MINIO_ACCESS_KEY
MINIO_SECRET_KEY = settings.config.MINIO_SECRET_KEY
MINIO_BUCKET = settings.config.MINIO_BUCKET
MINIO_SECURE = settings.config.MINIO_SECURE


class StorageManager:
    def __init__(self):
        self.client = Minio(
            MINIO_ENDPOINT,
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_SECURE
        )
        logger.info("Connection to " + MINIO_ENDPOINT)
        # Create bucket, if not exist
        found = self.client.bucket_exists(MINIO_BUCKET)
        if not found:
            self.client.make_bucket(MINIO_BUCKET)
            logger.info("Create bucket " + MINIO_BUCKET)
        else:
            logger.info("Found bucket " + MINIO_BUCKET)

    def list_objects(self, path_name, recursive=True):
        """
        List objects in a path. (List recursively by default)
        :param path_name: str
        :param recursive: boolean
        :return: [(object, tags)]
        """
        client = self.client
        objects = client.list_objects(MINIO_BUCKET, prefix=path_name, recursive=recursive)
        objs = []
        print('list_objects', path_name)
        for obj in objects:
            attr = "-"
            # tags = Tags.new_object_tags()
            # tags["Project"] = "Project One"
            # tags["User"] = "jsmith"
            # client.set_object_tags(obj.bucket_name, obj.object_name, tags)
            mtags = None
            if obj.is_dir:
                attr = "D"
            else:
                mtags = self.get_object_tags(obj.object_name)
            print('-', attr, obj.bucket_name, obj.object_name)
            print('--', mtags)
            # tobj.tags = tags
            objs.append((obj, mtags))

        return objs

    def get_object_content(self, path_name):
        """
        Get content of an object
        :param path_name:
        :return: content
        """
        client = self.client
        try:
            res = client.get_object(MINIO_BUCKET, path_name)
            content = res.data.decode()
        finally:
            res.close()
            res.release_conn()
        return content

    def get_object_tags(self, path_name, bucket_name=MINIO_BUCKET) -> dict:
        client = self.client
        try:
            tags = client.get_object_tags(bucket_name, path_name)
        except S3Error:
            tags = None
        return tags

    def set_object_tags(self, path_name, tags):
        """
        set tags of an object. Use tags=None to remove all tags
        :param path_name:
        :param tags: {'key':'value'}
        :return:
        """
        client = self.client
        if tags == None:
            client.delete_object_tags(MINIO_BUCKET, path_name)
            return
        mtags = Tags.new_object_tags()
        if tags is not None:
            for k, v in tags.items():
                mtags[k] = v
        client.set_object_tags(MINIO_BUCKET, path_name, mtags)
        return

    def put_object_stream(self, path_name, stream: io.BytesIO, length, tags=None):
        client = self.client
        res = client.put_object(
            MINIO_BUCKET, path_name, stream, length, part_size=5*1024*1024
        )
        self.set_object_tags(path_name, tags)
        return res

    def put_object_content(self, path_name, content, tags=None):
        """
        Put content to an object
        :param path_name:
        :param content:
        :param tags: {'key':'value'}
        :return:
        """
        byte_content = content.encode()
        return self.put_object_stream(path_name, io.BytesIO(byte_content), len(byte_content), tags)

    def remove_object(self, path_name):
        """
        Remove object
        :param path_name:
        :return:
        """
        client = self.client
        res = client.remove_object(MINIO_BUCKET, path_name)
        return res

    def stat_object(self, path_name):
        """
        Get status of an object
        :param path_name:
        :return: minio object
        """
        client = self.client
        res = client.stat_object(MINIO_BUCKET, path_name)
        return res

    def put_file_object(self, path_name, file_path, tags=None):
        """
        Put a file as anstorage_man = StorageManager() object
        :param path_name:
        :param file_path:
        :param tags: {'key':'value'}
        :return:
        """
        client = self.client
        res = client.fput_object(MINIO_BUCKET, path_name, file_path)
        self.set_object_tags(path_name, tags)
        return res

    def get_file_object(self, path_name, file_path=None):
        """
        Get an object as a file
        :param path_name:
        :param file_path:
        :return: (file_path, minio object)
        """
        client = self.client
        if file_path is None:
            temp_file = NamedTemporaryFile(delete=False)
            file_path = temp_file.name
        try:
            res = client.fget_object(MINIO_BUCKET, path_name, file_path)
            mtags = self.get_object_tags(res.object_name)
        except ValueError:
            return (None, None, None)
        except S3Error:
            return (None, None, None)
        return (file_path, res, mtags)
