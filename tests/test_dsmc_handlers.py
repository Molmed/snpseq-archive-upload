
import json
import mock
import shutil
import subprocess
import tarfile
import uuid

from nose.tools import *
from mockproc import mockprocess

from tornado.testing import *
from tornado.web import Application
from tornado.escape import json_encode

from arteria.web.state import State

from archive_upload.app import routes
from archive_upload import __version__ as archive_upload_version
from archive_upload.handlers.dsmc_handlers import VersionHandler, UploadHandler, StatusHandler, ReuploadHandler, CreateDirHandler, GenChecksumsHandler, ReuploadHelper, BaseDsmcHandler, ArchiveException, CompressArchiveHandler
from archive_upload.lib.jobrunner import LocalQAdapter
from archive_upload.lib.utils import FileUtils
from tests.test_utils import TestUtils, DummyConfig


class TestDsmcHandlers(AsyncHTTPTestCase):

    API_BASE="/api/1.0"

    dummy_config = DummyConfig()

    runner_service = LocalQAdapter(nbr_of_cores=2, whitelisted_warnings = dummy_config["whitelisted_warnings"], interval = 2, priority_method = "fifo")

    def get_app(self, config=None):
        return Application(
            routes(
                config=config or self.dummy_config,
                runner_service=self.runner_service))

    def test_version(self):
        """
        Test version.
        """
        response = self.fetch(self.API_BASE + "/version")

        expected_result = { "version": archive_upload_version }

        self.assertEqual(response.code, 200)
        self.assertEqual(json.loads(response.body), expected_result)

    def test__validate_runfolder_exists_ok(self):
        is_valid = UploadHandler._validate_runfolder_exists("testrunfolder", self.dummy_config["monitored_directory"])
        self.assertTrue(is_valid)

    def test__validate_runfolder_exists_not_ok(self):
        not_valid = UploadHandler._validate_runfolder_exists("non-existant", self.dummy_config["monitored_directory"])
        self.assertFalse(not_valid)

    def test__verify_unaligned(self):
        root = "tests/resources/unaligned_dir"
        exists = CreateDirHandler._verify_unaligned(root + "/link")
        self.assertTrue(exists)

    def test__verify_unaligned_missing_link(self):
        root = "tests/resources/unaligned_dir"
        exists = CreateDirHandler._verify_unaligned(root + "/no_link")
        self.assertFalse(exists)

    def test__verify_unaligned_error_link(self):
        root = "tests/resources/unaligned_dir"
        exists = CreateDirHandler._verify_unaligned(root + "/error_link")
        self.assertFalse(exists)

    def test__verify_unaligned_file_instead_of_link(self):
        root = "tests/resources/unaligned_dir"
        exists = CreateDirHandler._verify_unaligned(root + "/error_file")
        self.assertFalse(exists)

    @mock.patch("archive_upload.lib.jobrunner.LocalQAdapter.start", autospec=True)
    def test_start_upload(self, mock_start):
        job_id = 24
        mock_start.return_value = job_id

        response = self.fetch(self.API_BASE + "/upload/test_archive", method="POST", allow_nonstandard_methods=True)
        json_resp = json.loads(response.body)

        self.assertEqual(response.code, 202)
        self.assertEqual(json_resp["job_id"], job_id)
        self.assertEqual(json_resp["service_version"], archive_upload_version)

        import socket
        self.assertEqual(json_resp["archive_host"], socket.gethostname())

        expected_link = "http://localhost:{0}/api/1.0/status/".format(self.get_http_port())
        self.assertTrue(expected_link in json_resp["link"])
        self.assertEqual(json_resp["state"], State.STARTED)

        root_dir = self.dummy_config["log_directory"]
        created_dir = "{}/dsmc_{}".format(root_dir, "test_archive")

        self.assertTrue(os.path.exists(created_dir))
        os.rmdir(created_dir)

    @mock.patch("archive_upload.handlers.dsmc_handlers.BaseDsmcHandler._is_valid_log_dir", autospec=True)
    def test_raise_exception_on_log_dir_problem(self, mock__is_valid_log_dir):
        mock__is_valid_log_dir.return_value = False
        response = self.fetch(self.API_BASE + "/upload/test_archive", method="POST", allow_nonstandard_methods=True)

        self.assertEqual(response.code, 500)

    # TODO: Should probably test more thoroughly our modifications of the LocalQAdaptor as well.
    @mock.patch("archive_upload.lib.jobrunner.LocalQAdapter.status", autospec=True)
    def test_check_status(self, mock_status):
        mock_status.return_value = State.DONE
        response = self.fetch(self.API_BASE + "/status/1")
        json_resp = json.loads(response.body)
        self.assertEqual(json_resp["state"], State.DONE)
        mock_status.assert_called_with(self.runner_service, "1")

    def test_create_dir(self):
        archive_path = "./tests/resources/archives/testrunfolder_archive"

        # Base case
        body = {"remove": "True"}
        response = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        json_resp = json.loads(response.body)

        first_created_at = os.path.getctime(archive_path)
        # Dirty workaround so we do not try to create the dir too quickly
        # the second time.
        import time
        time.sleep(1)

        # Ensure nothing is excluded
        self.assertEqual(json_resp["state"], State.DONE)
        self.assertTrue(os.path.exists(archive_path))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory1")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory3")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory2", "file.bar")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory2", "file.bin")))

        # Exclude parameters in POST request
        body = {"remove": "True", "exclude_dirs": "directory3, someotherdir", "exclude_extensions": ".bin,.hmm"}
        response = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        json_resp = json.loads(response.body)

        # Ensure that only extensions and dirs in the POST request are excluded
        self.assertEqual(json_resp["state"], State.DONE)
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory1")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory2", "file.bar")))
        self.assertFalse(os.path.exists(os.path.join(archive_path, "directory3")))
        self.assertFalse(os.path.exists(os.path.join(archive_path, "directory2", "file.bin")))

        # Should fail due to folder already existing
        body = {"remove": "False"}
        response = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        json_resp = json.loads(response.body)

        self.assertEqual(json_resp["state"], State.ERROR)

        # Check that the dir is recreated
        os.mkdir(os.path.join(archive_path, "remove-me"))

        body = {"remove": "True"}
        response = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        json_resp = json.loads(response.body)

        self.assertEqual(json_resp["state"], State.DONE)
        second_created_at = os.path.getctime(archive_path)
        self.assertTrue(first_created_at < second_created_at)
        self.assertFalse(os.path.exists(os.path.join(archive_path, "remove-me")))

        shutil.rmtree(archive_path)

    def test_create_dir_on_biotank(self):
        body = {"remove": "False"}
        header = {"Host": "biotank42"}
        root = self.dummy_config["monitored_directory"]
        runfolder = "testrunfolder"
        path_to_runfolder = os.path.abspath(os.path.join(root, runfolder))

        with mock.patch("archive_upload.handlers.dsmc_handlers.CreateDirHandler._verify_unaligned") as mock__unaligned:
            mock__unaligned.return_value = False
            response = self.fetch(self.API_BASE + "/create_dir/" + runfolder, method="POST", body=json_encode(body), headers=header)

        mock__unaligned.assert_called_with(path_to_runfolder)
        self.assertEqual(response.code, 500)
        json_resp = json.loads(response.body)
        self.assertEqual(json_resp["state"], State.ERROR)

    def test_create_dir_missing_body(self):
        resp = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", allow_nonstandard_methods=True)
        self.assertEqual(resp.code, 400)
        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.ERROR)

    def test_create_dir_missing_remove(self):
        body = {"foo": "bar"}
        resp = self.fetch(self.API_BASE + "/create_dir/testrunfolder", method="POST", body=json_encode(body))
        self.assertEqual(resp.code, 400)
        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.ERROR)

    @mock.patch("archive_upload.lib.jobrunner.LocalQAdapter.start", autospec=True)
    def test_generate_checksum(self, mock_start):
        job_id = 42
        mock_start.return_value = job_id

        path_to_archive = os.path.abspath(os.path.join(self.dummy_config["path_to_archive_root"], "test_archive"))
        log_dir = os.path.abspath(self.dummy_config["log_directory"])
        checksum_log = os.path.join(log_dir, "checksum.log")
        filename = "checksums_prior_to_pdc.md5"

        response = self.fetch(self.API_BASE + "/gen_checksums/test_archive", method="POST", allow_nonstandard_methods=True) #body=json_encode(body))
        json_resp = json.loads(response.body)

        self.assertEqual(json_resp["state"], State.STARTED)
        self.assertEqual(json_resp["job_id"], job_id)

        expected_cmd = "cd {} && /usr/bin/find -L . -type f ! -path './{}' -exec /usr/bin/md5sum {{}} + > {}".format(path_to_archive, filename, filename)
        mock_start.assert_called_with(self.runner_service, expected_cmd, nbr_of_cores=1, run_dir=log_dir, stdout=checksum_log,  stderr=checksum_log)

    def test_reupload_handler(self):
        job_id = 27

        with \
            mock.patch \
                ("archive_upload.handlers.dsmc_handlers.ReuploadHelper.get_pdc_descr",\
                autospec=True) as mock_get_pdc_descr, \
            mock.patch \
                ("archive_upload.handlers.dsmc_handlers.ReuploadHelper.get_pdc_filelist",\
                autospec=True) as mock_get_pdc_filelist, \
            mock.patch \
                ("archive_upload.handlers.dsmc_handlers.ReuploadHelper.get_local_filelist",\
                autospec=True) as mock_get_local_filelist, \
            mock.patch \
                ("archive_upload.handlers.dsmc_handlers.ReuploadHelper.get_files_to_reupload",\
                autospec=True) as mock_get_files_to_reupload, \
            mock.patch("archive_upload.handlers.dsmc_handlers.ReuploadHelper.reupload",\
                autospec=True) as mock_reupload:

            mock_get_pdc_descr.return_value = "abc123"
            mock_get_pdc_filelist.return_value = "{'foo': 123}"
            mock_get_local_filelist.return_value = "{'foo': 123, 'bar': 456}"
            mock_get_files_to_reupload.return_value = "{'bar': 456}"
            mock_reupload.return_value = job_id

            resp = self.fetch(self.API_BASE + "/reupload/test_archive", method="POST",
            allow_nonstandard_methods=True)

        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.STARTED)
        self.assertEqual(json_resp["job_id"], job_id)

    # Successful test
    def test_get_pdc_descr(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        # FIXME: this dsmc-output can not be removed, or the test will fail
        self.scripts.append("dsmc", returncode=0,
                            script="""#!/bin/bash
cat tests/resources/dsmc_output/dsmc_descr.txt
""")

        with self.scripts:
            archive_path = "/data/mm-xart002/runfolders/johanhe_test_0809_001-AG2UJ_archive"
            descr = helper.get_pdc_descr(archive_path, dsmc_log_dir="")

        self.assertEqual(descr, "e374bd6b-ab36-4f41-94d3-f4eaea9f30d4")


    @raises(ArchiveException)
    def test_get_pdc_descr_failing_proc(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        self.scripts.append("dsmc", returncode=10)

        with self.scripts:
            archive_path = "/foo"
            descr = helper.get_pdc_descr(archive_path, dsmc_log_dir="")

    @raises(ArchiveException)
    def test_get_pdc_descr_no_results(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        self.scripts.append("dsmc", returncode=0, script="""#!/bin/bash
echo apa
""")

        with self.scripts:
            archive_path = "foobar"
            descr = helper.get_pdc_descr(archive_path, dsmc_log_dir="")

    def test_get_pdc_filelist(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        self.scripts.append("dsmc", returncode=0,
                            script="""#!/bin/bash
cat tests/resources/dsmc_output/dsmc_pdc_filelist.txt
""")

        with self.scripts:
            archive_path = "/data/mm-xart002/runfolders/johanhe_test_0809_001-AG2UJ_archive"
            filelist = helper.get_pdc_filelist(archive_path, "e374bd6b-ab36-4f41-94d3-f4eaea9f30d4", dsmc_log_dir="")

        with open("tests/resources/dsmc_output/dsmc_pdc_converted_filelist.txt") as f:
            nr_of_files = 0
            for line in f:
                size, name = line.split()
                self.assertEqual(int(filelist[name]), int(size))
                nr_of_files += 1

            self.assertEqual(len(filelist.keys()), nr_of_files)

    @raises(ArchiveException)
    def test_get_pdc_filelist_failing_proc(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        self.scripts.append("dsmc", returncode=13)

        with self.scripts:
            archive_path = "foo"
            filelist = helper.get_pdc_filelist(archive_path, "foo-bar", dsmc_log_dir="")

    @raises(ArchiveException)
    def test_get_pdc_filelist_no_result(self):
        self.scripts = mockprocess.MockProc()
        helper = ReuploadHelper()

        self.scripts.append("dsmc", returncode=0, script="""#!/bin/bash
echo uggla
""")
        with self.scripts:
            archive_path = "foo"
            filelist = helper.get_pdc_filelist(archive_path, "foo-bar", dsmc_log_dir="")

    def test_get_local_filelist(self):
        helper = ReuploadHelper()
        path = "tests/resources/archives/archive_from_pdc"

        cmd = "find {} -type f -exec du -b {{}} \;".format(path)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        du_out, du_err = p.communicate()
        du_out = du_out.splitlines()

        files = helper.get_local_filelist(path)

        for line in du_out:
            size, filename = line.split()
            path = os.path.join(path, filename)

            self.assertEqual(files[filename], int(size))

        self.assertEqual(len(files.keys()), len(du_out))

    @raises(ArchiveException)
    def test_get_local_filelist_no_result(self):
        uniq_id = str(uuid.uuid4())
        tmpdir = "/tmp/testcase-{}".format(uniq_id)
        os.mkdir(tmpdir)
        helper = ReuploadHelper()
        files = helper.get_local_filelist(tmpdir)
        os.rmdir(tmpdir)

    def test_get_files_to_reupload(self):
        helper = ReuploadHelper()

        local_files = {"foo": 23, "bar": 46}
        uploaded_files = {"foo": 23}
        expected = ["bar"]
        result = helper.get_files_to_reupload(local_files, uploaded_files)
        self.assertItemsEqual(expected, result)

        uploaded_files = {"foo": 44}
        expected = ["foo", "bar"]
        result = helper.get_files_to_reupload(local_files, uploaded_files)
        self.assertItemsEqual(expected, result)

        local_files = {"foo": 44}
        uploaded_files = {"foo": 44}
        expected = []
        result = helper.get_files_to_reupload(local_files, uploaded_files)
        self.assertItemsEqual(expected, result)

    def test_reupload(self):
        helper = ReuploadHelper()
        uniq_id = "test"
        dsmc_log_dir = "/tmp/foo"
        dsmc_log_file = "foolog"
        descr = "foodescr"
        run_dir = "foodir"
        uniq_id = str(uuid.uuid4())
        reupload_file = "/tmp/test_reupload-{}".format(uniq_id)

        exp_id = 72

        class MyRunner(object):
            def start(self, cmd, nbr_of_cores, run_dir, stdout=dsmc_log_file, stderr=dsmc_log_file):
                self.components = cmd.split("=")
                self.cmd = cmd
                return exp_id

        runsrv = MyRunner()

        local_files = {"foo": 23, "bar": 46, "uggla": 72}
        uploaded_files = {"foo": 23, "uggla": 15}
        exp_upload = ['"bar"\n', '"uggla"\n']

        reupload_files = helper.get_files_to_reupload(local_files, uploaded_files)

        def my_tmp_file(ReuploadHelper, component):
            return reupload_file

        with mock.patch \
                ("archive_upload.handlers.dsmc_handlers.ReuploadHelper._tmp_file",\
                autospec=True) as mock__tmp_file:
            mock__tmp_file.side_effect = my_tmp_file
            res_id = helper.reupload(reupload_files, descr, dsmc_log_dir, runsrv)

        self.assertEqual(res_id, exp_id)
        self.assertEqual(runsrv.components[-1], descr)

        with open(reupload_file) as f:
            uploaded = f.readlines()

        import sets
        uploaded = sets.Set(uploaded)
        exp_upload = sets.Set(exp_upload)
        self.assertEqual(len(uploaded.symmetric_difference(exp_upload)), 0)

    def test_compress_archive_full(self):
        root = self.dummy_config["path_to_archive_root"]
        archive_path = os.path.join(root, "johanhe_test_archive")
        original = os.path.join(root, "johanhe_test_runfolder")

        shutil.rmtree(archive_path, ignore_errors=True)
        shutil.copytree(original, archive_path)

        resp = self.fetch(self.API_BASE + "/compress_archive/johanhe_test_archive", method="POST",
                          allow_nonstandard_methods=True)


        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.DONE)

        self.assertFalse(os.path.exists(os.path.join(archive_path, "RunInfo.xml")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "Config")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "SampleSheet.csv")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "johanhe_test_archive.tar.gz")))

        shutil.rmtree(archive_path)

    def test_compress_archive_mini(self):
        root = self.dummy_config["path_to_archive_root"]
        archive_path = os.path.join(root, "testrunfolder_archive_tmp")
        original = os.path.join(root, "testrunfolder_archive_input")

        shutil.rmtree(archive_path, ignore_errors=True)
        shutil.copytree(original, archive_path)

        resp = self.fetch(self.API_BASE + "/compress_archive/testrunfolder_archive_tmp", method="POST",
                          allow_nonstandard_methods=True)

        json_resp = json.loads(resp.body)
        self.assertEqual(json_resp["state"], State.DONE)

        self.assertTrue(os.path.exists(os.path.join(archive_path, "file.csv")))
        self.assertFalse(os.path.exists(os.path.join(archive_path, "file.bin")))
        self.assertFalse(os.path.exists(os.path.join(archive_path, "directory2")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "directory3")))
        self.assertTrue(os.path.exists(os.path.join(archive_path, "testrunfolder_archive_tmp.tar.gz")))

        shutil.rmtree(archive_path)

    def test_compress_archive_exclude(self):
        """
        Don't exclude anything
        """
        root = self.dummy_config["path_to_archive_root"]
        archive_path = os.path.join(root, "testrunfolder_archive_tmp")
        original = os.path.join(root, "testrunfolder_archive_input")

        try:
            shutil.rmtree(archive_path, ignore_errors=True)
            shutil.copytree(original, archive_path)

            # update the config to exclude nothing from archive
            local_config = TestUtils.DUMMY_CONFIG.copy()
            self.dummy_config = TestUtils.DUMMY_CONFIG#Create a copy to not change the real config.
            self.dummy_config["exclude_from_tarball"] = []
            resp = self.fetch(
                self.API_BASE + "/compress_archive/" + os.path.basename(archive_path),
                method="POST",
                allow_nonstandard_methods=True)

            json_resp = json.loads(resp.body)
            tarball_archive_path = os.path.join(
                archive_path,
                "{}.tar.gz".format(
                    os.path.basename(archive_path)))

            self.assertEqual(State.DONE, json_resp["state"])
            self.assertEqual(archive_upload_version, json_resp["service_version"])
            self.assertTrue(os.path.exists(tarball_archive_path))
            self.assertListEqual([os.path.relpath(tarball_archive_path, archive_path)], os.listdir(archive_path))

            # verify that all files in the original file tree are present in the tarball
            entries_in_archive = FileUtils.source_paths_from_tarball(tarball_archive_path, original)
            entries_in_original = \
                FileUtils.list_all_paths(original) + [original]
            self.assertListEqual(
                sorted(map(os.path.normpath, entries_in_original)),
                sorted(map(os.path.normpath, entries_in_archive)))
        except Exception as e:
            raise
        finally:
            shutil.rmtree(archive_path)
            TestUtils.DUMMY_CONFIG = local_config

    @mock.patch("archive_upload.handlers.dsmc_handlers.os.path.isfile", autospec=True)
    def test_rename_log_file_no_file(self, mock_isfile):
        log_directory = "/log/directory/name_archive"
        expected_log_name = "/log/directory/name_archive/dsmc_output"
        mock_isfile.return_value = False
        self.assertEqual(BaseDsmcHandler._rename_log_file(log_directory), expected_log_name)

    def test_rename_log_file_exist(self):
        with mock.patch('archive_upload.handlers.dsmc_handlers.os.rename') as mock_rename, \
                mock.patch('archive_upload.handlers.dsmc_handlers.os.path.isfile') as mock_isfile, \
                mock.patch('archive_upload.handlers.dsmc_handlers.os.path.getmtime') as mock_getmtime:
         mock_isfile.return_value = True
         log_directory = "/log/directory/name_archive"
         expected_log_name = "/log/directory/name_archive/dsmc_output"
         mock_getmtime.return_value = "timestamp"
         self.assertEqual(BaseDsmcHandler._rename_log_file(log_directory), expected_log_name)
         mock_rename.assert_called_once_with("/log/directory/name_archive/dsmc_output",
                                                "/log/directory/name_archive/dsmc_output.timestamp")
