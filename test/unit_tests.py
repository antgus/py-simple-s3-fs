import unittest

import shutil
from src.fs import ObjectStore, LocalObjectStore
from typing import List
import random

LOCAL_DISK_TEST_FOLDER = "/tmp/py-simple-s3-fs/tests/"


class ObjectStoreUnitTests(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        self._reset_state()

    def test_run_tests(self):
        store = LocalObjectStore()
        self.run_tests(store, LOCAL_DISK_TEST_FOLDER)

    def _reset_state(self):
        if len(LOCAL_DISK_TEST_FOLDER) < 14:
            raise AssertionError(f"Fail safe check, are you sure the local test folder is: {LOCAL_DISK_TEST_FOLDER}. Sanity checking to avoid deleting important folders.")

        print(f"Deleting local disk test folder: {LOCAL_DISK_TEST_FOLDER}")
        try:
            shutil.rmtree(LOCAL_DISK_TEST_FOLDER)
        except FileNotFoundError:
            pass  # no need to raise this The folder may not exist and that doesn't matter.

    def run_tests(self, store: ObjectStore, folder: str):
        """
        We run all tests under this so that we can easily run the same set of tests for any given ObjectStore.
        """
        tests = [
            self._test_put_object_and_get_object,
            self._test_open_read,
            self._test_open_write,
            self._test_ls,
            self._test_exists,
            self._test_exists_list,
            self._test_rm
        ]
        for test in tests:
            self._reset_state()
            test(store, folder)

    def _test_put_object_and_get_object(self, store: ObjectStore, folder: str):
        for path in self._make_paths(store, folder):
            data = self._make_payload()
            store.put_object(path, data.encode())
            o = store.get_object(path)
            self.assertEqual(data, o.decode())

    def _test_open_read(self, store: ObjectStore, folder: str):
        for path in self._make_paths(store, folder):
            data = self._make_payload()
            store.put_object(path, data.encode())
            with store.open(path, "r") as f:
                read_data = f.read()
                self.assertEqual(data, read_data)

    def _test_open_write(self, store: ObjectStore, folder: str):
        for path in self._make_paths(store, folder):
            data = self._make_payload()
            with store.open(path, "w") as f:
                f.write(data)
            self.assertEqual(data, store.get_object(path).decode())

    def _test_ls(self, store: ObjectStore, folder: str):
        folders = [
            "",
            "folder/",
            "folder/subfolder/"
        ]
        filenames = ["_lala", "wee.txt"]
        for f in folders:
            for fname in filenames:
                path = store.path_join(folder, f, fname)
                data = self._make_payload()
                store.put_object(path, data.encode())

        t1_path = store.path_join(folder, "")
        expected = sorted([store.path_join(folder, f) for f in ["_lala", "wee.txt", "folder"]])
        self.assertEqual(expected, store.ls(t1_path))

        t2_path = store.path_join(folder, "folder/")
        expected = sorted([store.path_join(folder, "folder/", f) for f in ["_lala", "wee.txt", "subfolder"]])
        print(expected)
        print(store.ls(t2_path))
        self.assertEqual(expected, store.ls(t2_path))

    def _test_exists(self, store: ObjectStore, folder: str):
        path = store.path_join(folder, "f1.txt")
        self.assertFalse(store.exists(path))
        store.put_object(path, self._make_payload().encode())
        self.assertTrue(store.exists(path))

    def _test_exists_list(self, store: ObjectStore, folder: str):
        paths = [store.path_join(folder, f) for f in ["f1.txt", "f2.txt"]]
        self.assertEquals([], store.exists_list(paths))
        store.put_object(paths[0], self._make_payload().encode())
        self.assertEquals([paths[0]], store.exists_list(paths))
        store.put_object(paths[1], self._make_payload().encode())
        self.assertEquals(paths, store.exists_list(paths))

    def _test_rm(self, store: ObjectStore, folder: str):
        data = self._make_payload()
        p1 = store.path_join(folder, "f1.txt")
        p2 = store.path_join(folder, "f2.txt")
        store.put_object(p1, data.encode())
        store.put_object(p2, data.encode())
        self.assertTrue(store.exists(p1))
        self.assertTrue(store.exists(p2))
        store.rm(p1)
        self.assertFalse(store.exists(p1))
        self.assertTrue(store.exists(p2))
        store.rm(p2)
        self.assertFalse(store.exists(p2))

    @staticmethod
    def _make_paths(store: ObjectStore, folder: str) -> List[str]:
        return [store.path_join(folder, p) for p in [
            "my_file.txt",
            "folder1/my_file.txt",
            "folder2/__weee",
            "folder1/folder2/something.gz"
        ]]

    @staticmethod
    def _make_payload():
        return "payload_" + str(random.randint(0, 100000))
