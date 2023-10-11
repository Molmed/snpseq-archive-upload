
class TestUtils:

    DUMMY_CONFIG = {
        "monitored_directory": "tests/resources/",
        "whitelisted_warnings": ["ANS1809W", "ANS2000W"], 
        "log_directory": "tests/resources/dsmc_output/", 
        "path_to_archive_root": "tests/resources/archives/",
        "exclude_from_tarball": ["Config", "SampleSheet.csv", "file.csv", "directory3"],
        "tsm_mock_enabled": False
    }

class DummyConfig(dict):

    def __init__(self):
        super(DummyConfig, self).__init__(TestUtils.DUMMY_CONFIG)

    @staticmethod
    def get_app_config():
        return TestUtils.DUMMY_CONFIG.copy()
