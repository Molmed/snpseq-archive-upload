import datetime
import errno
import json
import logging
import os
import re
import subprocess
import shutil
import tarfile
import uuid

from arteria.exceptions import ArteriaUsageException
from arteria.web.state import State
from arteria.web.handlers import BaseRestHandler

from archive_upload import __version__ as version
from archive_upload.lib.jobrunner import LocalQAdapter

log = logging.getLogger(__name__)


class BaseDsmcHandler(BaseRestHandler):

    """
    Base handler for dsmc upload operations.
    """

    def initialize(self, config, runner_service):
        """
        Ensures that any parameters feed to this are available
        to subclasses.

        :param config: configuration used by the service
        :param runner_service: runner service to use. Must fulfill `archive_upload.lib.jobrunner.JobRunnerAdapter` interface
        """
        self.config = config
        self.runner_service = runner_service

    @staticmethod
    def _validate_runfolder_exists(runfolder, monitored_dir):
        """
        Validate that the runfolder exists under monitored directories
        :param runfolder: The runfolder to check for
        :param monitored_dir: The root in which the runfolder should exist
        :return: True if this is a valid runfolder
        """

        if os.path.isdir(monitored_dir):
            sub_folders = [name for name in os.listdir(monitored_dir)
                           if os.path.isdir(os.path.join(monitored_dir, name))]
            return runfolder in sub_folders
        else:
            return False

    @staticmethod
    def _is_valid_log_dir(log_dir):
        """
        Check if the log dir is valid. Right now only checks it is a directory.
        :param: log_dir to check
        :return: True is valid dir, else False
        """
        return os.path.isdir(log_dir)

    @staticmethod
    def _rm_empty_dirs(path, remove_root=True):
        """
        Recursively removes empty sub directories.
        :param path: Path to remove
        :param remove_root: Whether or not to remove the root directory
        """
        files = os.listdir(path)

        if files:
            for f in files:
                fullpath = os.path.join(path, f)

                if os.path.isdir(fullpath):
                    BaseDsmcHandler._rm_empty_dirs(fullpath)

        # If root dir now is empty - remove it
        files = os.listdir(path)

        if len(files) == 0 and remove_root:
            log.info("Removing empty folder: {}".format(path))
            os.rmdir(path)

    def abort(self, msg, code=500):
        log.debug(msg)
        self.set_status(code, reason=msg)
        response_data = {
            "service_version": version,
                "state": State.ERROR,
                "msg": msg}
        self.write_object(response_data)
        self.finish()


class VersionHandler(BaseDsmcHandler):

    """
    Get the version of the service
    """

    def get(self):
        """
        Returns the version of the dsmc service
        """
        self.write_object({"version": version})


class ReuploadHelper(object):

    """
    Helper class for the ReuploadHandler. Methods put here mainly to faciliate easier testing.
    """

    def get_pdc_descr(self, path_to_archive, dsmc_log_dir):
        """
        Fetches the archive `description` label from PDC.

        :param path_to_archive: The path to the archive uploaded that we want to get the description for
        :return: A dsmc description if successful, otherwise a FOO
        """
        log.info("Fetching description for latest upload of {} to PDC...".format(path_to_archive))
        cmd = "export DSM_LOG={} && dsmc q ar {}".format(dsmc_log_dir, path_to_archive)
        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        dsmc_out, _ = p.communicate()
        dsmc_out = dsmc_out.splitlines()

        if p.returncode != 0:
            raise ArteriaUsageException(
                "Error when getting description from PDC. dsmc returned != 0. Output:".format(dsmc_out))

        log.debug("Raw output from dsmc: {}".format(dsmc_out))

        uploaded_versions = [line.strip() for line in dsmc_out if path_to_archive in line]

        if not uploaded_versions:
            raise ArteriaUsageException(
                "Error when getting description from PDC. No descriptions available for {}".format(path_to_archive))

        log.debug(
            "Found the following uploaded versions of this archive: {}".format(uploaded_versions))

        # Uploads are chronologically sorted, with the latest upload last.
        latest_upload = uploaded_versions[-1]

        # We need the description of this upload: the last field. E.g.:
        # 4,096  B  01/10/2017 16:47:24
        # /data/mm-xart002/runfolders/johanhe_test_0809_001-AG2UJ_archive Never
        # a33623ba-55ad-4034-9222-dae8801aa65e
        latest_descr = latest_upload.split()[-1]
        log.debug(
            "Latest uploaded version is {} with description {}".format(latest_upload, latest_descr))

        return latest_descr

    def _parse_name_size(self, line, search_string):
        """
        Searches through the string `line` and tries to parse out the filename and the byte size of the file.
        A (TSM) line can look like:
        4,096  B  2017-07-27 17.48.34    /data/mm-xart002/runfolders/johanhe_test_0809_001-AG2UJ_archive/Config Never e374bd6b-ab36-4f41-94d3-f4eaea9f30d4
        but varies, depending on the environment's locale. Size can e.g. be "4 096".

        :param line: TSM output string to parse, usually come
        :param search_string: A magic search string that we know will exist in `line`, to make it easier to split `line` into parts
        :return A tuple with the filename and the file's byte size
        """
        elements = line.split(" B ")
        size = elements[0].strip()

        if "," in size:
            byte_size = size.replace(",", "")
        elif " " in size:
            byte_size = size.replace(" ", "")
        else:
            byte_size = size

        # We can't be completely sure what format the timestamp will be returned with.
        # And we can not be 100% sure what format the description will have either, at least in the future.
        # This will have to do for now.
        substr = re.search("{}(.*) Never ".format(search_string), line)
        filename = ("{}{}".format(search_string, substr.group(1))).strip()

        return (filename, byte_size)

    def get_pdc_filelist(self, path_to_archive, descr, dsmc_log_dir):
        """
        Gets the files and their sizes from PDC for a certain path (archive), with a specific description.

        :param path_to_archive: The path to the archive
        :param descr: The description label for the uploaded archive
        :return The dict `uploaded_files` containing a mapping between uploaded file and size in bytes
        """
        log.info("Fetching remote filelist for {} from PDC...".format(path_to_archive))
        cmd = "export DSM_LOG={} && dsmc q ar {}/ -subdir=yes -description={}".format(
            dsmc_log_dir, path_to_archive, descr)

        p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)

        dsmc_out, _ = p.communicate()
        dsmc_out = dsmc_out.splitlines()

        if p.returncode != 0:
            raise ArteriaUsageException(
                "Error when getting filelist from PDC. Output:".format(dsmc_out))

        # We're only interested in the lines from the dsmc output that contains the
        # path to the archive.
        matched_lines = [line.strip() for line in dsmc_out if path_to_archive in line]

        if not matched_lines:
            raise ArteriaUsageException(
                "Error when getting filelist from PDC. No files uploaded for {}".format(path_to_archive))

        log.debug("Uploaded files to PDC: {}".format(matched_lines))

        uploaded_files = {}

        # We need to convert the sizes to a common format for easier comparison with local size.
        for line in matched_lines:
            filename, byte_size = self._parse_name_size(line, path_to_archive)

            # NB A potential error here is if the same file has been uploaded multiple times with the same descriptions.
            # It is then a bit ambigious what to do. TSM sorts and returns them in chronological order though,
            # so we will just keep refering to the last uploaded version of the file.
            if filename in uploaded_files.keys():
                log.info(
                    "Duplicate uploads of file {} with description {} encountered.".format(filename, descr))

            uploaded_files[filename] = int(byte_size)

        log.debug("Previously uploaded files for the archive are: {}".format(uploaded_files))

        return uploaded_files

    def get_local_filelist(self, path_to_archive):
        """
        Gets the list of all files and their sizes in the local archive.

        :param path_to_archive: The path to the local archive
        :return: The dict `local_files` that maps between local file and size in bytes
        """
        log.info("Generating local filelist for {}...".format(path_to_archive))
        local_files = {}
        for root, directories, filenames in os.walk(path_to_archive):
            for filename in filenames:
                full_path = os.path.join(root, filename)
                local_size = os.path.getsize(full_path)
                local_files[full_path] = int(local_size)

        if not local_files:
            raise ArteriaUsageException(
                "Error when generating local filelist. No files found for {}".format(path_to_archive))

        log.debug("Local files for the archive are {}".format(local_files))

        return local_files

    def get_files_to_reupload(self, local_files, uploaded_files):
        """
        Compare the list of local and uploaded files. If the size in byte differs,
        or if the file exists locally, but not remotely, then it should be re-uploaded.

        :param local_files: Dict local files -> size in bytes
        :param uploaded_files: Dict of remote files -> size in bytes
        :return: List `reupload_files` with the path to all files that needs reuploading
        """
        reupload_files = []
        for name, size in local_files.iteritems():
            if name in uploaded_files:
                assert isinstance(size, int), "Local file size needs to be of type int"
                assert isinstance(
                    uploaded_files[name], int), "Remote file size needs to be of type int"
                log.debug("Local file has been uploaded {}".format(name))

                if size != uploaded_files[name]:
                    log.info("Local file size {} doesn't match remote file size {} for file {}".format(
                        size, uploaded_files[name], name))
                    reupload_files.append(name)
                else:
                    log.debug("Local file ({}) size matches uploaded file size".format(name))
            else:
                log.info("Local file has NOT been uploaded {}".format(name))
                reupload_files.append(name)

        return reupload_files

    def reupload(self, reupload_files, descr, dsmc_log_dir, runner_service):
        """
        Tells `dsmc` to upload all files in the given filelist.

        :param reupload_files: List of files to reupload
        :param descr: The unique description of the already uploaded archive with missing files
        :param uniq_id: A uniq ID for this sessions DSMC interactions
        :param dsmc_log_dir: The dir where `dsmc` will write log files
        :param runner_service: The runner service to use
        :return: The LocalQ job id associated with this job
        """
        log.info("Will now reupload the following files: {}".format(reupload_files))

        reupload_file = self._tmp_file("archive-upload-reupload")

        with open(reupload_file, 'wa') as f:
            for r in reupload_files:
                f.write('"{}"\n'.format(r))

        log.debug("Written files to reupload to {}".format(reupload_file))

        output_file = "{}/dsmc_output".format(dsmc_log_dir)

        cmd = "export DSM_LOG={} && dsmc archive -filelist={} -description={}".format(
            dsmc_log_dir, reupload_file, descr)
        log.debug("Running command {}".format(cmd))
        job_id = runner_service.start(
            cmd, nbr_of_cores=1, run_dir=dsmc_log_dir, stdout=output_file, stderr=output_file)

        return job_id

    def _tmp_file(self, component):
        uniq_id = str(uuid.uuid4())
        return os.path.join("/tmp", "{}-{}".format(component, uniq_id))


class ReuploadHandler(BaseDsmcHandler):

    """
    Handler for (re-)uploading missing files for a certain archive already uploaded to PDC.
    Useful when e.g. a previous upload was interrupted, or if new files should be added.
    """

    def post(self, runfolder_archive):
        """
        Compares local copy of the runfolder archive with the latest uploaded version.
        If any files are missing on the remote (PDC) side then they will be uploaded.
        Job is run in the background to be polled by the status endpoint.

        :param runfolder_archive: the archive we want to re-upload
        :return: HTTP 200 if nothing to reupload, HTTP 202 if reupload started successfully, with a `job_id` to be used for later polling,
                 HTTP 500 if unexpected error detected.

        """
        monitored_dir = self.config["path_to_archive_root"]
        helper = ReuploadHelper()

        if not self._validate_runfolder_exists(runfolder_archive, monitored_dir):
            msg = "Error when validating runfolder. {} is not found under {}.".format(
                runfolder_archive, monitored_dir)
            self.abort(msg)

        path_to_archive = os.path.join(monitored_dir, runfolder_archive)
        dsmc_log_root_dir = self.config["log_directory"]

        if not self._is_valid_log_dir(dsmc_log_root_dir):
            msg = "Error when validating log dir. {} is not a directory.".format(dsmc_log_root_dir)
            self.abort(msg)

        dsmc_log_dir = "{}/dsmc_{}".format(dsmc_log_root_dir, runfolder_archive)

        if not os.path.exists(dsmc_log_dir):
            os.makedirs(dsmc_log_dir)

        # Fetch the description of the last uploaded version of this archive
        descr = helper.get_pdc_descr(path_to_archive, dsmc_log_dir)

        # Get the local and remote filelist, and then get the list of files
        # that are missing on remote side, or differs in byte size.
        # NB. Uploaded list contains folders as well, but when we check local
        # content we only look at the files, and ignore the folders.
        uploaded_files = helper.get_pdc_filelist(path_to_archive, descr, dsmc_log_dir)
        local_files = helper.get_local_filelist(path_to_archive)
        reupload_files = helper.get_files_to_reupload(local_files, uploaded_files)

        # Upload the missing files with the same description previously used.
        if reupload_files:
            job_id = helper.reupload(reupload_files, descr, dsmc_log_dir, self.runner_service)
            log.debug("Reupload job_id {}".format(job_id))

            status_end_point = "{0}://{1}{2}".format(
                self.request.protocol,
                self.request.host,
                self.reverse_url("status", job_id))

            response_data = {
                "job_id": job_id,
                "service_version": version,
                "link": status_end_point,
                "state": State.STARTED,
                "dsmc_log_dir": dsmc_log_dir}

            self.set_status(202, reason="started reuploading")
        else:
            log.debug("Nothing to do - everything already uploaded.")

            response_data = {
                "service_version": version,
                "state": State.ERROR,
                "dsmc_log_dir": dsmc_log_dir}

            self.set_status(400, reason="nothing to reupload")

        self.write_object(response_data)


class UploadHandler(BaseDsmcHandler):

    """
    Handler for uploading an archive to PDC.
    """

    def post(self, runfolder_archive):
        """
        Tells `dsmc` to upload `runfolder_archive` to PDC, with a uniquely generated description label.
        Job is run in the background to be polled by the status endpoint.

        :param runfolder_archive: the name of the archive that we want to upload
        :return: HTTP 202 if the upload as started successfully, with a `job_id` to be used for later status polling,
                 HTTP 500 if an unexpected error was encountered
        """

        monitored_dir = self.config["path_to_archive_root"]

        if not self._validate_runfolder_exists(runfolder_archive, monitored_dir):
            msg = "Error when validating runfolder. {} is not found under {}".format(
                runfolder_archive, monitored_dir)
            self.abort(msg)

        path_to_archive = os.path.join(monitored_dir, runfolder_archive)
        dsmc_log_root_dir = self.config["log_directory"]
        uniq_id = str(uuid.uuid4())

        if not self._is_valid_log_dir(dsmc_log_root_dir):
            msg = "Error when validating log dir. {} is not a directory.".format(dsmc_log_root_dir)
            self.abort(msg)

        dsmc_log_dir = "{}/dsmc_{}".format(dsmc_log_root_dir, runfolder_archive)

        if not os.path.exists(dsmc_log_dir):
            os.makedirs(dsmc_log_dir)

        output_file = "{}/dsmc_output".format(dsmc_log_dir)

        log.info("Uploading {} to PDC...".format(path_to_archive))
        cmd = "export DSM_LOG={} && dsmc archive {}/ -subdir=yes -description={}".format(
            dsmc_log_dir, path_to_archive, uniq_id)
        job_id = self.runner_service.start(
            cmd, nbr_of_cores=1, run_dir=dsmc_log_dir, stdout=output_file, stderr=output_file)

        status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

        response_data = {
            "job_id": job_id,
            "service_version": version,
            "link": status_end_point,
            "state": State.STARTED,
            "dsmc_log_dir": dsmc_log_dir}

        self.set_status(202, reason="started processing")
        self.write_object(response_data)


class GenChecksumsHandler(BaseDsmcHandler):

    """
    Handler for generating checksums for an archive before uploading to PDC.
    """

    def post(self, runfolder_archive):
        """
        Calculates the MD5 checksums for each file in the runfolder archive, before uploading to PDC.
        Job is run in the background to be polled by the status endpoint.

        :param runfolder_archive: Name of the runfolder archive
        :returns: HTTP 202 if checksum job has started successfully, with a `job_id` to be used in later polling,
                  HTTP 500 if an unexpected error was encountered
        """
        path_to_archive_root = os.path.abspath(self.config["path_to_archive_root"])
        log_dir = os.path.abspath(self.config["log_directory"])
        checksum_log = os.path.abspath(os.path.join(log_dir, "checksum.log"))

        if not self._validate_runfolder_exists(runfolder_archive, path_to_archive_root):
            msg = "Error when validating runfolder. {} is not found under {}".format(
                runfolder_archive, path_to_archive_root)
            self.abort(msg)

        path_to_archive = os.path.join(path_to_archive_root, runfolder_archive)
        filename = "checksums_prior_to_pdc.md5"

        cmd = "cd {} && /usr/bin/find -L . -type f ! -path './{}' -exec /usr/bin/md5sum {{}} + > {}".format(
            path_to_archive, filename, filename)
        log.info("Generating checksums for {}".format(path_to_archive))
        log.debug("Will now execute command {}".format(cmd))
        job_id = self.runner_service.start(
            cmd, nbr_of_cores=1, run_dir=log_dir, stdout=checksum_log, stderr=checksum_log)

        status_end_point = "{0}://{1}{2}".format(
            self.request.protocol,
            self.request.host,
            self.reverse_url("status", job_id))

        response_data = {
            "job_id": job_id,
            "service_version": version,
            "link": status_end_point,
            "state": State.STARTED}

        self.set_status(202, reason="started processing")
        self.write_object(response_data)


class CreateDirHandler(BaseDsmcHandler):

    """
    Handler for creating an archive to upload.
    """

    @staticmethod
    def _verify_unaligned(srcdir):
        """
        Check that the archive contains the `Unaligned` symlink when running on biotanks.
        The link should point to a proper directory.

        :param srcdir: The path to the archive which we should investigate
        :return: True if `srcdir` contains a symlink `Unaligned` that points to a directory,
                 False otherwise
        """
        log.debug("Validating Unaligned...")

        unaligned_link = os.path.join(srcdir, "Unaligned")
        unaligned_dir = os.path.abspath(unaligned_link)

        if not os.path.exists(unaligned_link) or not os.path.islink(unaligned_link):
            log.info(
                "Expected link {} doesn't seem to exist or is broken. Aborting.".format(unaligned_link))
            return False
        elif not os.path.exists(unaligned_dir) or not os.path.isdir(unaligned_dir):
            log.info("Expected directory {} doesn't seem to exist. Aborting.".format(unaligned_dir))
            return False

        return True

    @staticmethod
    def _verify_dest(destdir, remove=False):
        """
        Check if the proposed new archive already exists, and if the operator wants to remove it then do so.

        :param destdir: Path to the archive to create
        :param remove: Boolean that specifies whether or not we should remove `destdir` if it already exists
        :return: True if the archive doesn't exist, or if it was removed successfully,
                 False otherwise
        """
        log.debug("Checking to see if {} exists".format(destdir))

        if os.path.exists(destdir):
            if remove:
                log.debug(
                    "Archive directory {} already exists. Operator requested to remove it.".format(destdir))
                shutil.rmtree(destdir)
                return True
            else:
                log.debug("Archive directory {} already exists. Aborting.".format(destdir))
                return False
        else:
            return True

    @staticmethod
    def _create_archive(oldtree, newtree, exclude_dirs=[], exclude_extensions=[]):
        """
        Create a new runfolder archive named `<runfolder>_archive` by iterating through
        the runfolder and symlinking each file. If the service has been configured to
        exclude certain directories or file extensions then those directories and symlinks
        will be ignored.

        :param oldtree: Path to the runfolder
        :param newtree: Path to the archive which we are going to create
        :param exclude_dir: List of directory names to exclude from the archive
        :param exclude_extensions: List of file extensions to exclude from the archive
        """
        try:
            content = os.listdir(oldtree)

            for entry in content:
                oldpath = os.path.join(oldtree, entry)
                newpath = os.path.join(newtree, entry)

                if os.path.isdir(oldpath) and entry not in exclude_dirs:
                    log.debug("Creating new dir {} because {} is not in exclude_dirs={}".format(
                        newpath, entry, exclude_dirs))
                    os.mkdir(newpath)
                    CreateDirHandler._create_archive(
                        oldpath, newpath, exclude_dirs, exclude_extensions)
                elif os.path.isfile(oldpath):
                    _, ext = os.path.splitext(oldpath)

                    if ext not in exclude_extensions:
                        log.debug("Creating new symlink {} because {} is not in exclude_extensions={}".format(
                            newpath, ext, exclude_extensions))
                        os.symlink(oldpath, newpath)
                    else:
                        log.debug(
                            "Skipping symlinking {} because {} files are excluded".format(oldpath, ext))
                else:
                    log.debug(
                        "Skipping to create an archive directory of {} because it is excluded".format(oldpath))
        except OSError, msg:
            errmsg = "Error when creating archive directory: {}".format(msg)
            log.debug(errmsg)
            raise ArteriaUsageException(errmsg)

    def post(self, runfolder):
        """
        Create a directory to be used for archiving.

        :param runfolder: name of the runfolder we want to create an archive dir of
        :param remove: boolean to indicate if we should remove previous archive
        :return: HTTP 200 if runfolder archive was created successfully,
                 HTTP 500 otherwise
        """
        monitored_dir = self.config["monitored_directory"]
        path_to_runfolder = os.path.abspath(os.path.join(monitored_dir, runfolder))
        path_to_archive_root = self.config["path_to_archive_root"]
        path_to_archive = os.path.abspath(
            os.path.join(path_to_archive_root, runfolder) + "_archive")

        exclude_dirs = self.config["exclude_dirs"]
        exclude_extensions = self.config["exclude_extensions"]

        missing_rm_msg = "Need to provide a True/False for the `remove` field in the HTTP body."

        try:
            request_data = json.loads(self.request.body)
            remove = eval(request_data["remove"])  # str2bool
        except (ValueError, KeyError):
            self.abort(missing_rm_msg)

        if not isinstance(remove, bool):
            self.abort(missing_rm_msg)

        if not self._validate_runfolder_exists(runfolder, monitored_dir):
            msg = "Error encountered when validating runfolder. {} is not under {}".format(
                runfolder, monitored_dir)
            self.abort(msg)

        my_host = self.request.headers.get('Host')

        if "biotank" in my_host and not self._verify_unaligned(path_to_runfolder):
            msg = "Error when validating Unaligned. Unaligned symlink {} broken or missing.".format(
                path_to_runfolder, "Unaligned")
            self.abort(msg)

        if not self._verify_dest(path_to_archive, remove):
            msg = "Error when validating destination path {} (remove={})".format(
                path_to_archive, remove)
            self.abort(msg)

        try:
            log.info("Creating a new archive {}...".format(path_to_archive))
            os.mkdir(path_to_archive)
            self._create_archive(
                path_to_runfolder, path_to_archive, exclude_dirs, exclude_extensions)
        except ArteriaUsageException, msg:
            self.abort("Error when creating archive dir: {}".format(msg))

        response_data = {"service_version": version, "state": State.DONE}

        self.set_status(200, reason="Finished processing.")
        self.write_object(response_data)


class CompressArchiveHandler(BaseDsmcHandler):

    """
    Handler for compressing certain files in the archive before uploading.
    """

    def post(self, archive):
        """
        Create a gziped tarball of most files in the archive, with the exception of
        certain excluded files and directories that are to be kept as-is in the archive.

        :param archive: The name of the archive which we should pack together
        :return: HTTP 200 if the tarball was created successfully,
                 HTTP 500 otherwise

        """
        path_to_archive_root = self.config["path_to_archive_root"]
        path_to_archive = os.path.abspath(os.path.join(path_to_archive_root, archive))

        if not self._validate_runfolder_exists(archive, path_to_archive_root):
            msg = "Error encountered when validating runfolder. {} is not under {}".format(
                archive, path_to_archive_root)
            self.abort(msg)

        tarball_path = "{}.tar.gz".format(os.path.join(path_to_archive, archive))

        log.debug("Checking to see if {} exists".format(tarball_path))

        if os.path.exists(tarball_path):
            msg = "Error when creating archive tarball. {} already exists.".format(tarball_path)
            self.abort(msg)

        def exclude_content(tarinfo):
            """
            Filter function when creating the tarball
            """
            name = os.path.basename(tarinfo.name)
            # The name field contains the path to the file relative to the
            # root dir of the archive, i.e. the path starts with "./".
            # Therefore the second element in the list split on "/"
            # will be the first subdir (if any) inside the archive.
            first_dir = tarinfo.name.split("/")[1]

            # Don't include the file if it matches our list of
            # files to exclude, or if the first dir in its path
            # matches one of the dir names in our exception list.
            for exclude in exclude_from_tarball:
                if exclude == name or exclude == first_dir:
                    return None

            return tarinfo

        exclude_from_tarball = self.config["exclude_from_tarball"]
        log.info("Creating tarball {}...".format(tarball_path))

        with tarfile.open(name=tarball_path, mode="w:gz", dereference=True) as tar:
            tar.add(path_to_archive, arcname="./", recursive=True, filter=exclude_content)

        log.info("Removing files from {} that were added to {}".format(
            path_to_archive_root, tarball_path))

        # Remove files that we added to the tarball.
        with tarfile.open(tarball_path) as tar:
            for member in tar.getmembers():
                try:
                    filepath = os.path.normpath(
                        os.path.join(path_to_archive_root, archive, member.name))

                    if os.path.isfile(filepath):
                        os.remove(filepath)
                    elif member.name != "." and os.path.isdir(filepath):
                        self._rm_empty_dirs(filepath)

                except OSError, e:
                    msg = "Error when creating archive tarball. Could not remove file {}: {}".format(
                        filepath, e)
                    self.abort(msg)

        response_data = {"service_version": version, "state": State.DONE}
        self.set_status(200, reason="Finished creating the tarball")
        self.write_object(response_data)


class StatusHandler(BaseDsmcHandler):

    """
    Get the status of one or all jobs.
    """

    def get(self, job_id):
        """
        Get the status of the specified job_id, or if now id is given, the
        status of all jobs.
        :param job_id: to check status for (set to empty to get status for all)
        """

        if job_id:
            status = {"state": self.runner_service.status(job_id)}
        else:
            # TODO: Update the correct status for all jobs; the filtering in jobrunner
            # doesn't work here.
            all_status = self.runner_service.status_all()
            status_dict = {}
            for k, v in all_status.iteritems():
                status_dict[k] = {"state": v}
            status = status_dict

        self.write_json(status)

# class StopHandler(BaseDsmcHandler):
#    """
#    Stop one or all jobs.
#    """
#
#    def post(self, job_id):
#        """
#        Stops the job with the specified id.
#        :param job_id: of job to stop, or set to "all" to stop all jobs
#        """
#        try:
#            if job_id == "all":
#                log.info("Attempting to stop all jobs.")
#                self.runner_service.stop_all()
#                log.info("Stopped all jobs!")
#                self.set_status(200)
#            elif job_id:
#                log.info("Attempting to stop job: {}".format(job_id))
#                self.runner_service.stop(job_id)
#                self.set_status(200)
#            else:
#                ArteriaUsageException("Unknown job to stop")
#        except ArteriaUsageException as e:
#            log.warning("Failed stopping job: {}. Message: ".format(job_id, e.message))
#            self.send_error(500, reason=e.message)
