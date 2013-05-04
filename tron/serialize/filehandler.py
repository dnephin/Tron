"""
Tools for managing and properly closing file handles.
"""
import logging
import os
import os.path
import shutil
import sys
from subprocess import PIPE, Popen
import time


from tron.utils.dicts import OrderedDict

log = logging.getLogger(__name__)


class NullFileHandle(object):
    """A No-Op object that supports a File interface."""
    closed = True
    @classmethod
    def write(cls, _):
        pass
    @classmethod
    def close(cls):
        pass


# TODO: use observer instead of passing a manager
class FileHandleWrapper(object):
    """Acts as a proxy to file handles.  Wrap a file handle and stores
    access time and metadata.  These objects should only be created
    by FileHandleManager. Do not instantiate them on their own.
    """
    __slots__ = ['manager', 'name', 'last_accessed', '_fh']

    def __init__(self, manager, name):
        self.manager = manager
        self.name = name
        self.last_accessed = time.time()
        self._fh = NullFileHandle

    def close(self):
        self.close_wrapped()
        self.manager.remove(self)

    def close_wrapped(self):
        """Close only the underlying file handle."""
        self._fh.close()
        self._fh = NullFileHandle

    def write(self, content):
        """Write content to the fh. Re-open if necessary."""
        if self._fh == NullFileHandle:
            try:
                self._fh = open(self.name, 'a')
            except IOError, e:
                log.error("Failed to open %s: %s", self.name, e)
                return

        self.last_accessed = time.time()
        self._fh.write(content)
        self.manager.update(self)

    def last_accessed_before(self, threshold_time):
        return self.last_accessed < threshold_time

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        self.close()


class FileHandleManager(object):
    """Creates FileHandleWrappers, closes handles when they have
    been inactive for a period of time, and transparently re-open the next
    time they are needed. All files are opened in append mode.
    """

    def __init__(self, wrapper_class, max_idle_time=60):
        """ Create a new instance.
            max_idle_time - max idle time in seconds
        """
        self.wrapper_class = wrapper_class
        self.max_idle_time = max_idle_time
        self.cache = OrderedDict()

    def clear(self):
        for fh_wrapper in self.cache.values():
            self.remove(fh_wrapper)

    def open(self, filename):
        """Retrieve a file handle from the cache based on name.  Returns a
        FileHandleWrapper. If the handle is not in the cache, create a new
        instance.
        """
        if filename in self.cache:
            return self.cache[filename]
        wrapper = self.cache[filename] = self.wrapper_class(self, filename)
        return wrapper

    def cleanup(self):
        """Close any file handles that have been idle for longer than
        max_idle_time. time_func is primary used for testing.
        """
        if not self.cache:
            return

        threshold = time.time() - self.max_idle_time
        for name, fh_wrapper in self.cache.items():
            if not fh_wrapper.last_accessed_before(threshold):
                break
            fh_wrapper.close()

    def remove(self, fh_wrapper):
        """Remove the fh_wrapper from the cache and access_order."""
        if fh_wrapper.name in self.cache:
            del self.cache[fh_wrapper.name]

    def update(self, fh_wrapper):
        """Remove and re-add the file handle to the cache so that it's keys
        are still ordered by last access. Calls cleanup() to remove any file
        handles that have been idle for too long.
        """
        self.remove(fh_wrapper)
        self.cache[fh_wrapper.name] = fh_wrapper
        self.cleanup()


class FileManagerSingleton(object):

    @classmethod
    def get_instance(cls):
        manager = FileHandleManager(FileHandleWrapper)
        cls.get_instance = classmethod(lambda cls: manager)
        return manager

    orig_get_instance = get_instance

    @classmethod
    def reset(cls):
        cls.get_instance().clear()
        cls.get_instance = cls.orig_get_instance


class OutputStreamSerializer(object):
    """Manage writing to and reading from files in a directory hierarchy."""

    def __init__(self, base_path):
        self.base_path = os.path.join(*base_path)
        if not os.path.exists(self.base_path):
            os.makedirs(self.base_path)

    def full_path(self, filename):
        return os.path.join(self.base_path, filename)

    # TODO: do not use subprocess
    def tail(self, filename, num_lines=None):
        """Tail a file using `tail`."""
        path = self.full_path(filename)
        if not path or not os.path.exists(path):
            return []
        if not num_lines:
            num_lines = sys.maxint

        try:
            cmd = ('tail', '-n', str(num_lines), path)
            tail_sub = Popen(cmd, stdout=PIPE)
            return list(line.rstrip() for line in tail_sub.stdout)
        except OSError, e:
            log.error("Could not tail %s: %s" % (path, e))
            return []

    def open(self, filename):
        """Return a FileHandleWrapper for the output path."""
        path = self.full_path(filename)
        return FileManagerSingleton.get_instance().open(path)


class OutputPath(object):
    """A list like object used to construct a file path for output. The
    file path is constructed by joining the base path with any additional
    path elements.
    """
    __slots__ = ['base', 'parts']

    def __init__(self, base='.', *path_parts):
        self.base = base
        self.parts = list(path_parts or [])

    def append(self, part):
        self.parts.append(part)

    def __iter__(self):
        yield self.base
        for p in self.parts:
            yield p

    def __str__(self):
        return os.path.join(*self)

    def clone(self, *parts):
        """Return a new OutputPath object which has a base of the str value
        of this object.
        """
        return type(self)(str(self), *parts)

    def delete(self):
        """Remove the directory and its contents."""
        try:
            shutil.rmtree(str(self))
        except OSError, e:
            log.warn("Failed to delete %s: %s" % (self, e))

    def __eq__(self, other):
        return self.base == other.base and self.parts == other.parts

    def __ne__(self, other):
        return not self == other
