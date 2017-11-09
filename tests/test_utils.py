
class TestUtils:

    DUMMY_CONFIG = {
        "monitored_directory": "tests/resources/",
        "whitelisted_warnings": ["ANS1809W", "ANS2000W"], 
        "log_directory": "tests/resources/dsmc_output/", 
        "path_to_archive_root": "tests/resources/archives/",
        "exclude_from_tarball": ["Config", "SampleSheet.csv", "file.csv", "directory3"], 
        "exclude_dirs": ["directory1"],
        "exclude_extensions": [".bar"]

    }

class DummyConfig:
    def __getitem__(self, key):
        return TestUtils.DUMMY_CONFIG[key]

