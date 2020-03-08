"""Provides shared memory for direct access across processes.

The API of this package is currently provisional. Refer to the
documentation for details.

Adapted from the python 3.8 stdlib to work with python 3.6
"""

import ctypes
import mmap
import os
import secrets

librt = ctypes.cdll.LoadLibrary('librt.so.1')

_O_CREX = os.O_CREAT | os.O_EXCL

# FreeBSD (and perhaps other BSDs) limit names to 14 characters.
_SHM_SAFE_NAME_LENGTH = 14

# Shared memory block name prefix
_SHM_NAME_PREFIX = '/psm_'


def shm_open(name, flags, mode):
    return librt.shm_open(ctypes.c_char_p(name.encode('utf-8')), ctypes.c_uint32(flags), ctypes.c_uint32(mode))


def shm_unlink(name):
    return librt.shm_unlink(ctypes.c_char_p(name.encode('utf-8')))


def _make_filename():
    "Create a random filename for the shared memory object."
    # number of random bytes to use for name
    nbytes = (_SHM_SAFE_NAME_LENGTH - len(_SHM_NAME_PREFIX)) // 2
    assert nbytes >= 2, '_SHM_NAME_PREFIX too long'
    name = _SHM_NAME_PREFIX + secrets.token_hex(nbytes)
    assert len(name) <= _SHM_SAFE_NAME_LENGTH
    return name


class SharedMemory:
    """Creates a new shared memory block or attaches to an existing
    shared memory block.

    Every shared memory block is assigned a unique name.  This enables
    one process to create a shared memory block with a particular name
    so that a different process can attach to that same shared memory
    block using that same name.

    As a resource for sharing data across processes, shared memory blocks
    may outlive the original process that created them.  When one process
    no longer needs access to a shared memory block that might still be
    needed by other processes, the close() method should be called.
    When a shared memory block is no longer needed by any process, the
    unlink() method should be called to ensure proper cleanup."""

    # Defaults; enables close() and unlink() to run without errors.
    _name = None
    _fd = -1
    _mmap = None
    _buf = None
    _flags = os.O_RDWR
    _mode = 0o600
    _prepend_leading_slash = True

    def __init__(self, name=None, create=False, size=0):
        if not size >= 0:
            raise ValueError("'size' must be a positive integer")
        if create:
            self._flags = _O_CREX | os.O_RDWR
        if name is None and not self._flags & os.O_EXCL:
            raise ValueError("'name' can only be None if create=True")

        # POSIX Shared Memory

        if name is None:
            while True:
                name = _make_filename()
                try:
                    self._fd = shm_open(
                        name,
                        self._flags,
                        mode=self._mode
                    )
                except FileExistsError:
                    continue
                self._name = name
                break
        else:
            name = "/" + name if self._prepend_leading_slash else name
            self._fd = shm_open(
                name,
                self._flags,
                mode=self._mode
            )
            self._name = name
        try:
            if create and size:
                os.ftruncate(self._fd, size)
            stats = os.fstat(self._fd)
            size = stats.st_size
            self._mmap = mmap.mmap(self._fd, size)
        except OSError:
            self.unlink()
            raise

        self._size = size
        self._buf = memoryview(self._mmap)

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __reduce__(self):
        return (
            self.__class__,
            (
                self.name,
                False,
                self.size,
            ),
        )

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name!r}, size={self.size})'

    @property
    def buf(self):
        "A memoryview of contents of the shared memory block."
        return self._buf

    @property
    def name(self):
        "Unique name that identifies the shared memory block."
        reported_name = self._name
        if self._prepend_leading_slash:
            if self._name.startswith("/"):
                reported_name = self._name[1:]
        return reported_name

    @property
    def size(self):
        "Size in bytes."
        return self._size

    def close(self):
        """Closes access to the shared memory from this instance but does
        not destroy the shared memory block."""
        if self._buf is not None:
            self._buf.release()
            self._buf = None
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def unlink(self):
        """Requests that the underlying shared memory block be destroyed.

        In order to ensure proper cleanup of resources, unlink should be
        called once (and only once) across all processes which have access
        to the shared memory block."""
        if self._name:
            shm_unlink(self._name)
