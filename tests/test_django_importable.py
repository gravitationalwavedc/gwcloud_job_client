from unittest import TestCase


class TestDjangoImportable(TestCase):
    def test_django_importable(self):
        # This case just tries to import client which makes sure that the client can import and load django correctly
        # in case the import order is unexpected changed
        import client
        client  # Linters begone
