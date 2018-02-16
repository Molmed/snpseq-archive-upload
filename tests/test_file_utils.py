import mock
import os
import unittest

from archive_upload.lib.utils import FileUtils
from tests.test_utils import DummyConfig


class TestFileUtils(unittest.TestCase):

    def setUp(self):
        self.dummy_config = DummyConfig()

    @mock.patch.object(FileUtils, "source_paths_from_tarball", new_callable=mock.MagicMock)
    def test_paths_duplicated_in_tarball(self, handler_mock):
        root = self.dummy_config["path_to_archive_root"]
        original = os.path.join(root, "testrunfolder_archive_input")
        tarball_paths = [
            os.path.join(original, "file.csv"),
            os.path.join(original, "directory3", "file.zip"),
            os.path.join(original, "directory3"),
            os.path.join(original, "directory2", "file.txt"),
            os.path.join(original, "directory2")
        ]
        handler_mock.return_value = tarball_paths
        duplicated_paths = FileUtils.paths_duplicated_in_tarball(None, original)
        self.assertListEqual(tarball_paths, duplicated_paths)
