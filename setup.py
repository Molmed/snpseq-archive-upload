from setuptools import setup, find_packages
from archive_upload import __version__
import os


def read_file(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

try:
    with open("requirements/prod", "r") as f:
        install_requires = [x.strip() for x in f.readlines()]
except IOError:
    install_requires = []

setup(
    name='archive-upload',
    version=__version__,
    description="Micro-service for uploading archives with DSMC",
    long_description=read_file('README.md'),
    keywords='bioinformatics',
    author='SNP&SEQ Technology Platform, Uppsala University',
    packages=find_packages(),
    include_package_data=True,
    entry_points={
        'console_scripts': ['archive-upload-ws = archive_upload.app:start']
    },
)
