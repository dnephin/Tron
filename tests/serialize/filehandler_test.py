import os
import shutil
from tempfile import NamedTemporaryFile, mkdtemp

import mock
from testify import TestCase, run, assert_equal, assert_not_in, assert_in
from testify import assert_not_equal, assert_is
from testify import setup, teardown, suite, setup_teardown

from tests.testingutils import autospec_method
from tron.serialize.filehandler import OutputStreamSerializer
from tron.serialize.filehandler import OutputPath, NullFileHandle
from tron.serialize import filehandler


class FileHandleWrapperTestCase(TestCase):

    @setup_teardown
    def patch_time(self):
        with mock.patch('tron.serialize.filehandler.time') as self.mock_time:
            self.mock_time.time.return_value = self.time = 123456789
            self.file = NamedTemporaryFile('r')
            self.manager = mock.create_autospec(filehandler.FileHandleManager)
            self.fh_wrapper = filehandler.FileHandleWrapper(
                self.manager, self.file.name)
            yield

    def test_init(self):
        assert_equal(self.fh_wrapper._fh, NullFileHandle)

    def test_close(self):
        # Test close without a write, no exception is good
        self.fh_wrapper.close()
        # Test close again, after already closed
        self.fh_wrapper.close()
        self.manager.remove.assert_called_with(self.fh_wrapper)

    def test_close_with_write(self):
        self.fh_wrapper.write("some things")
        self.fh_wrapper.close()
        assert_equal(self.fh_wrapper._fh, NullFileHandle)
        assert_equal(self.fh_wrapper.manager, self.manager)
        self.manager.remove.assert_called_with(self.fh_wrapper)

    def test_write(self):
        # Test write without a previous open
        self.mock_time.time.return_value = new_time = self.time + 10
        self.fh_wrapper.write("some things")

        assert self.fh_wrapper._fh
        assert_equal(self.fh_wrapper._fh.closed, False)
        assert_equal(self.fh_wrapper.last_accessed, new_time)

        # Test write after previous open
        self.mock_time.time.return_value = new_time = self.time + 20
        self.fh_wrapper.write("\nmore things")
        assert_equal(self.fh_wrapper.last_accessed, new_time)
        # TODO: mock writes
        self.fh_wrapper.close()
        with open(self.file.name) as fh:
            assert_equal(fh.read(), "some things\nmore things")

    def test_close_many(self):
        self.fh_wrapper.write("some things")
        self.fh_wrapper.close()
        self.fh_wrapper.close()

    def test_context_manager(self):
        with self.fh_wrapper as fh:
            fh.write("123")
        assert fh._fh.closed
        with open(self.file.name) as fh:
            assert_equal(fh.read(), "123")

    def test_last_accessed_before_true(self):
        assert self.fh_wrapper.last_accessed_before(self.time + 2)

    def test_last_accessed_before_false(self):
        assert not self.fh_wrapper.last_accessed_before(self.time - 2)


class FileManagerSingletonTestCase(TestCase):

    def test_single_instance(self):
        try:
            insta = filehandler.FileManagerSingleton.get_instance()
            instb = filehandler.FileManagerSingleton.get_instance()
            assert_is(insta, instb)
        finally:
            filehandler.FileManagerSingleton.reset()


class FileHandleManagerTestCase(TestCase):

    @setup
    def setup_fh_manager(self):
        self.file1 = NamedTemporaryFile('r')
        self.file2 = NamedTemporaryFile('r')
        self.wrapper = mock.create_autospec(filehandler.FileHandleWrapper)
        self.manager = filehandler.FileHandleManager(self.wrapper)

    def test_open_new(self):
        self.manager.open(self.file1.name)
        assert_in(self.file1.name, self.manager.cache)

    def test_open_cached(self):
        fh_wrapper = self.manager.open(self.file1.name)
        fh_wrapper2 = self.manager.open(self.file1.name)
        assert_is(fh_wrapper, fh_wrapper2)

    def test_cleanup_none(self):
        fh_wrapper = self.manager.open(self.file1.name)
        fh_wrapper.last_accessed_before.return_value = False
        self.manager.cleanup()
        assert not fh_wrapper.close.called

    def test_cleanup_single(self):
        fh_wrapper = self.manager.open(self.file1.name)
        fh_wrapper.last_accessed_before.return_value = True
        self.manager.cleanup()
        fh_wrapper.close.assert_called_with()

    def test_cleanup_many(self):
        fh_wrappers = [ mock.MagicMock() for _ in xrange(5) ]
        for wrapper, ret in zip(fh_wrappers, [True, True, True, False, False]):
            self.manager.cache[wrapper.name] = wrapper
            wrapper.last_accessed_before.return_value = ret

        self.manager.cleanup()
        for fh_wrapper in fh_wrappers[:3]:
            fh_wrapper.close.assert_called_with()

        for fh_wrapper in fh_wrappers[3:]:
            assert not fh_wrapper.close.called

    def test_remove(self):
        fh_wrapper = mock.MagicMock()
        self.manager.update(fh_wrapper)
        assert_in(fh_wrapper.name, self.manager.cache)
        self.manager.remove(fh_wrapper)
        assert_not_in(fh_wrapper.name, self.manager.cache)

    def test_remove_not_in_cache(self):
        fh_wrapper = mock.Mock()
        self.manager.remove(fh_wrapper)
        assert_not_in(fh_wrapper.name, self.manager.cache)

    def test_update(self):
        autospec_method(self.manager.cleanup)
        autospec_method(self.manager.remove)

        wrapper = mock.MagicMock()
        self.manager.update(wrapper)
        self.manager.cleanup.assert_called_with()
        self.manager.remove.assert_called_with(wrapper)
        assert_equal(self.manager.cache[wrapper.name], wrapper)


class OutputStreamSerializerTestCase(TestCase):

    @setup
    def setup_serializer(self):
        self.test_dir = mkdtemp()
        self.serial = OutputStreamSerializer([self.test_dir])
        self.filename = "STARS"
        self.content = "123\n456\n789"
        self.expected = self.content.split('\n')

    @teardown
    def teardown_test_dir(self):
        shutil.rmtree(self.test_dir)

    def _write_contents(self):
        with open(self.serial.full_path(self.filename), 'w') as f:
            f.write(self.content)

    def test_open(self):
        with self.serial.open(self.filename) as fh:
            fh.write(self.content)

        with open(self.serial.full_path(self.filename)) as f:
            assert_equal(f.read(), self.content)

    @suite('integration')
    def test_init_with_output_path(self):
        path = OutputPath(self.test_dir, 'one', 'two', 'three')
        stream = OutputStreamSerializer(path)
        assert_equal(stream.base_path, str(path))

    def test_tail(self):
        self._write_contents()
        assert_equal(self.serial.tail(self.filename), self.expected)

    def test_tail_num_lines(self):
        self._write_contents()
        assert_equal(self.serial.tail(self.filename, 1), self.expected[-1:])

    def test_tail_file_does_not_exist(self):
        file_dne = 'bogusfile123'
        assert_equal(self.serial.tail(file_dne), [])


class OutputPathTestCase(TestCase):

    @setup
    def setup_path(self):
        self.path = OutputPath('one', 'two', 'three')

    def test__init__(self):
        assert_equal(self.path.base, 'one')
        assert_equal(self.path.parts, ['two', 'three'])

        path = OutputPath('base')
        assert_equal(path.base, 'base')
        assert_equal(path.parts, [])

    def test__iter__(self):
        assert_equal(list(self.path), ['one', 'two', 'three'])

    def test__str__(self):
        # Breaks in windows probably,
        assert_equal('one/two/three', str(self.path))

    def test_append(self):
        self.path.append('four')
        assert_equal(self.path.parts, ['two', 'three', 'four'])

    def test_clone(self):
        new_path = self.path.clone()
        assert_equal(str(new_path), str(self.path))

        self.path.append('alpha')
        assert_equal(str(new_path), 'one/two/three')

        new_path.append('beta')
        assert_equal(str(self.path), 'one/two/three/alpha')

    def test_clone_with_parts(self):
        new_path = self.path.clone('seven', 'ten')
        assert_equal(list(new_path), ['one/two/three', 'seven', 'ten'])

    def test_delete(self):
        tmp_dir = mkdtemp()
        path = OutputPath(tmp_dir)
        path.delete()
        assert not os.path.exists(tmp_dir)

    def test__eq__(self):
        other = mock.Mock(base='one', parts=['two', 'three'])
        assert_equal(self.path, other)

    def test__ne__(self):
        other = mock.Mock(base='one/two', parts=['three'])
        assert_not_equal(self.path, other)

if __name__ == "__main__":
    run()
