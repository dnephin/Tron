import mock
from testify import TestCase, assert_equal, setup
from testify.assertions import assert_not_equal
from tron import actioncommand

from tron.actioncommand import ActionCommand
from tron.config import schema
from tron.serialize import filehandler

class ActionCommandTestCase(TestCase):

    @setup
    def setup_command(self):
        self.serializer = mock.create_autospec(filehandler.FileHandleManager)
        self.serializer.open.return_value = filehandler.NullFileHandle
        self.ac = ActionCommand("action.1.do", "do", self.serializer)

    def test_init(self):
        assert_equal(self.ac.state, ActionCommand.PENDING)

    def test_init_no_serializer(self):
        ac = ActionCommand("action.1.do", "do")
        ac.write_stdout("something")
        ac.write_stderr("else")
        assert_equal(ac.stdout, filehandler.NullFileHandle)
        ac.done()

    def test_started(self):
        assert self.ac.started()
        assert self.ac.start_time is not None
        assert_equal(self.ac.state, ActionCommand.RUNNING)

    def test_started_already_started(self):
        self.ac.started()
        assert not self.ac.started()

    def test_exited(self):
        self.ac.started()
        assert self.ac.exited(123)
        assert_equal(self.ac.exit_status, 123)
        assert self.ac.end_time is not None

    def test_exited_from_pending(self):
        assert self.ac.exited(123)
        assert_equal(self.ac.state, ActionCommand.FAILSTART)

    def test_exited_bad_state(self):
        self.ac.started()
        self.ac.exited(123)
        assert not self.ac.exited(1)

    def test_write_stderr_no_fh(self):
        message = "this is the message"
        # Test without a stderr
        self.ac.write_stderr(message)

    def test_write_stderr(self):
        message = "this is the message"
        serializer = mock.create_autospec(filehandler.FileHandleManager)
        fh = serializer.open.return_value = mock.create_autospec(
            filehandler.FileHandleWrapper)
        ac = ActionCommand("action.1.do", "do", serializer)

        ac.write_stderr(message)
        fh.write.assert_called_with(message)

    def test_done(self):
        self.ac.started()
        self.ac.exited(123)
        assert self.ac.done()

    def test_done_bad_state(self):
        assert not self.ac.done()

    def test_handle_errback(self):
        message = "something went wrong"
        self.ac.handle_errback(message)
        assert_equal(self.ac.state, ActionCommand.FAILSTART)
        assert self.ac.end_time

    def test_is_failed(self):
        assert not self.ac.is_failed

    def test_is_failed_true(self):
        self.ac.exit_status = 255
        assert self.ac.is_failed

    def test_is_complete(self):
        assert not self.ac.is_complete

    def test_is_complete_true(self):
        self.ac.machine.state = self.ac.COMPLETE
        assert self.ac.is_complete, self.ac.machine.state

    def test_is_done(self):
        self.ac.machine.state = self.ac.FAILSTART
        assert self.ac.is_done, self.ac.machine.state
        self.ac.machine.state = self.ac.COMPLETE
        assert self.ac.is_done, self.ac.machine.state


class CreateActionCommandFactoryFromConfigTestCase(TestCase):

    def test_create_default_action_command_no_config(self):
        config = ()
        factory = actioncommand.create_action_runner_factory_from_config(config)
        assert_equal(factory, actioncommand.NoActionRunnerFactory)

    def test_create_default_action_command(self):
        config = schema.ConfigActionRunner('none', None, None)
        factory = actioncommand.create_action_runner_factory_from_config(config)
        assert_equal(factory, actioncommand.NoActionRunnerFactory)

    def test_create_action_command_with_simple_runner(self):
        status_path, exec_path = '/tmp/what', '/remote/bin'
        config = schema.ConfigActionRunner('simple', status_path, exec_path)
        factory = actioncommand.create_action_runner_factory_from_config(config)
        assert_equal(factory.status_path, status_path)
        assert_equal(factory.exec_path, exec_path)

    def test__eq__true(self):
        first = actioncommand.SimpleActionRunnerFactory('a', 'b')
        second = actioncommand.SimpleActionRunnerFactory('a', 'b')
        assert_equal(first, second)

    def test__eq__false(self):
        first = actioncommand.SimpleActionRunnerFactory('a', 'b')
        second = actioncommand.SimpleActionRunnerFactory('a', 'c')
        assert_not_equal(first, second)
        assert_not_equal(first, None)
        assert_not_equal(first, actioncommand.NoActionRunnerFactory)
