from typing import List, Optional, Callable, Tuple
import s3fs
import os
import boto3
from dataclasses import dataclass
import shutil


class ObjectStoreException(Exception):
    pass


# interface
class ListQuery:
    pass


@dataclass
class S3Query(ListQuery):
    prefix: Optional[str] = None
    recursive: Optional[bool] = None


# Interface
class ObjectStore:
    def get_object(self, path: str) -> bytes:
        raise NotImplementedError

    def put_object(self, path: str, data: bytes) -> None:
        raise NotImplementedError

    def ls(self, path: str, query: ListQuery=None) -> List[str]:
        raise NotImplementedError

    def ls_filtered(self, path: str, query: ListQuery) -> List[str]:
        raise NotImplementedError

    def rm(self, path: str, recursive=False) -> None:
        raise NotImplementedError

    def exists(self, path: str) -> bool:
        raise NotImplementedError

    def exists_list(self, paths: List[str]) -> List[str]:
        return [p for p in paths if self.exists(p)]  # may be overriden for more efficient implementations

    def open(self, file, mode='r', **kwargs):
        raise NotImplementedError

    def path_join(self, p: str, *paths) -> str:
        raise NotImplementedError


class S3ObjectStore(ObjectStore):
    def __init__(self):
        self._s3 = boto3.client('s3')
        self._s3fs = s3fs.S3FileSystem()

    def exists(self, path: str) -> bool:
        return self._s3fs.exists(path)

    def get_object(self, path: str) -> bytes:
        with self.open(path, 'rb') as fp:
            return fp.read()

    def put_object(self, path: str, data: bytes) -> None:
        with self.open(path, 'wb') as fp:
            fp.write(data)

    def ls(self, path: str, query: ListQuery = None) -> List[str]:
        if query is None:
            return sorted(["s3://" + f for f in self._s3fs.ls(path)])
        else:
            return sorted(self._ls_query())

    def _ls_query(self, path: str, prefix: str, recursive: bool=False) -> List[str]:
        bucket, path_key = self._split_path(path)
        delim = "/" if not recursive else ""
        actual_prefix = path_key + prefix

        rsp = self._s3.list_objects_v2(
            Bucket=bucket,
            Delimiter=delim,
            Prefix=actual_prefix
        )
        if rsp['IsTruncated']:
            raise AssertionError("Truncated responses not supported yet")

        files = []
        if 'Contents' in rsp:
            files += [o['Key'] for o in rsp['Contents']]

        if 'CommonPrefixes' in rsp:
            files += [o['Prefix'] for o in rsp['CommonPrefixes']]

        # remove trailing "/" to match output of s3fs
        files = [f[:-1] if f.endswith("/") else f for f in files]

        return ["s3://" + bucket + "/" + f for f in files]

    def rm(self, path: str, recursive=False) -> None:
        self._s3fs.rm(path, recursive=recursive)

    def open(self, file, mode="r", **kwargs):
        return self._s3fs.open(file, mode, **kwargs)

    def path_join(self, p: str, *paths) -> str:
        """
        Adds "/" if necessary between path elements
        """
        all_paths = [p] + list(paths)
        all_paths = [p for p in all_paths if p != ""]
        for i in range(len(all_paths)-1):
            if not all_paths[i].endswith("/"):
                all_paths[i] += "/"
        return "".join(all_paths)

    def _split_path(self, path: str) -> Tuple[str, str]:
        assert(path.startswith("s3://"))
        parts = path[5:].split("/")
        bucket_name = parts[0]
        key = "/".join(parts[1:]) if len(parts) > 1 else ""
        return bucket_name, key


class LocalObjectStore(ObjectStore):
    def __init__(self):
        pass

    def exists(self, path: str) -> bool:
        return os.path.exists(path)

    def get_object(self, path: str) -> bytes:
        with self.open(path, 'rb') as fp:
            return fp.read()

    def put_object(self, path: str, data: bytes) -> None:
        """
            Creates directory if it doesn't exist
        """
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with self.open(path, 'wb') as fp:
            fp.write(data)

    def ls(self, path: str, query: ListQuery =None) -> List[str]:
        if query is None:
            return sorted([self.path_join(path, f) for f in os.listdir(path)])
        else:
            return sorted(self._ls_query(query))

    def _ls_query(self, path: str, prefix: str, recursive: bool=False) -> List[str]:
        if recursive:
            all_files = []
            for sub_path, subdirs, files in os.walk(path):
                for name in files:
                    all_files.append(os.path.join(sub_path, name))
        else:
            all_files = self.ls(path)
        files = [f for f in all_files if f.startswith(prefix)]
        files.sort()
        return files

    def rm(self, path: str, recursive=False) -> None:
        if recursive:
            if len(path) < 5:
                raise AssertionError(f"You probably didn't intend to recursively remove folder {path}")
            shutil.rmtree(path)
        else:
            os.remove(path)

    def open(self, file, mode='r', **kwargs):
        """
            Note: if mode is not read,
            a directory will be created if doesn't already exist
        """
        if mode !='r' and os.path.dirname(file) not in ["/", ""]:
            os.makedirs(os.path.dirname(file), exist_ok=True)
        return open(file, mode, **kwargs)

    def path_join(self, p: str, *paths) -> str:
        return os.path.join(p, *paths)


class MultiObjectStore(ObjectStore):
    def __init__(self):
        self.map_prefix_fs = {}

    def add_fs(self, fs: ObjectStore, prefix: Optional[str] = None) -> 'MultiObjectStore':
        """
        If prefix == None, it's used as the default file system
        :return: self
        """
        self.map_prefix_fs[prefix] = fs
        return self

    def put_object(self, path: str, data: bytes) -> None:
        self._fs(path).put_object(path, data)

    def get_object(self, path: str) -> bytes:
        return self._fs(path).get_object(path)

    def exists(self, path: str) -> bool:
        return self._fs(path).exists(path)

    def ls(self, path: str, query: ListQuery=None) -> List[str]:
        return self._fs(path).ls(path)

    def rm(self, path: str, recursive=False) -> None:
        return self._fs(path).rm(path, recursive)

    def open(self, file: str, mode='r', **kwargs) -> Callable:
        return self._fs(file).open(file, mode, **kwargs)

    def path_join(self, p: str, *paths) -> str:
        return self._fs(p).path_join(p, *paths)

    def _fs(self, path: str) -> ObjectStore:
        fs_list = [fs for prefix, fs in self.map_prefix_fs.items() if prefix is not None and path.startswith(prefix)]
        if len(fs_list) > 0:
            return fs_list[0]
        else:
            return self.map_prefix_fs[None]


class InMemoryObjectStore(ObjectStore):
    perPath_data = {}

    def get_object(self, path: str) -> bytes:
        return self.perPath_data[path]

    def put_object(self, path: str, data: bytes) -> None:
        self.perPath_data[path] = data
        pass

    def ls(self, path: str, query: ListQuery = None) -> List[str]:
        return self.perPath_data[path] # todo more efficient implementation

    def rm(self, path: str, recursive=False) -> None:
        pass

    def exists(self, path: str) -> bool:
        pass

    def open(self, file, mode='r', **kwargs):
        pass

    def path_join(self, p: str, *paths) -> str:
        pass


def create_multi_object_store() -> MultiObjectStore:
    return MultiObjectStore()\
        .add_fs(S3ObjectStore(), "s3://")\
        .add_fs(LocalObjectStore())


fs = create_multi_object_store()
