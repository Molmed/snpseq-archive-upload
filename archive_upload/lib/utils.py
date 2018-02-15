import os
import tarfile


class FileUtils(object):

    @staticmethod
    def list_all_paths(path, followlinks=False):
        """
        Recursively traverse the supplied folder with os.walk and return a list of full paths to all files and folders
        beneath the path. Essentially equivalent to `find path`. Note that symlinks pointing to files are
        returned as well.

        The returned list will be sorted in reverse lexical order, meaning that paths in subdirectories will come
        before the parent directories

        :param path: folder, beneath which to list all files
        :param followlinks: if True, follow symlinks to directories (default False)
        :return: a list of full paths discovered with os.walk
        """

        all_paths = []
        for dirpath, subdirs, dirfiles in os.walk(os.path.normpath(path), followlinks=followlinks):
            all_paths.extend(map(lambda f: os.path.join(dirpath, f), dirfiles + subdirs))
        return sorted(all_paths, reverse=True)

    @staticmethod
    def source_paths_from_tarball(tarball, path_to_source):
        """
        List the paths inside the tarball and return their full paths rooted at the supplied source path

        :param tarball: the tarball to list files from
        :param path_to_source: the path to the root of the source folder
        :return: a list of the paths inside the tarball, using the supplied source path as root
        """
        with tarfile.open(tarball) as tar:
            return map(
                lambda m: os.path.normpath(os.path.join(path_to_source, m.name)),
                tar.getmembers())

    @staticmethod
    def paths_duplicated_in_tarball(tarball, path_to_archive):
        """
        List the files and folders in the supplied folder and its subdirectories and return the paths
        that are present in the supplied tarball, assuming that the tarball is rooted
        at the supplied folder.

        :param tarball: a tarball whose members are rooted at the supplied path
        :param path_to_archive: path to search for files and folders duplicated in the tarball
        :return: a list of duplicated files and folders, sorted in reverse lexical order
        """
        # list the paths in the tarball and store as a set
        paths_in_tarball = set(FileUtils.source_paths_from_tarball(tarball, path_to_archive))
        paths_in_source_archive = FileUtils.list_all_paths(path_to_archive, followlinks=False)

        # duplicated paths are present in tarball and on disk, so take the intersection of the lists
        duplicated_paths = paths_in_tarball.intersection(paths_in_source_archive)

        return sorted(list(duplicated_paths), reverse=True)
