from pathlib import Path
from tempfile import TemporaryDirectory

import aiounittest


class TestArchiveJob(aiounittest.AsyncTestCase):
    async def test_archive_job(self):
        # Create a temporary directory to act as the job working directory
        with TemporaryDirectory() as temp_dir:
            # Create some files in the temporary directory
            for idx in range(10):
                (Path(temp_dir) / str(idx)).write_text(f"Test data {idx}")


