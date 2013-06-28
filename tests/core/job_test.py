import datetime
import mock

from testify import setup, teardown, TestCase, run, assert_equal
from testify import setup_teardown
from testify.assertions import assert_not_equal
from tests import mocks
from tests.assertions import assert_length, assert_call, assert_mock_calls
from tests.testingutils import Turtle, autospec_method
from tests import testingutils
from tron import node, event, actioncommand
from tron import scheduler
from tron.config import schema
from tron.core import job, jobrun
from tron.core.actionrun import ActionRun
from tron.serialize import filehandler


class JobTestCase(TestCase):

    @setup_teardown
    def setup_job(self):
        action_graph = mock.Mock(names=lambda: ['one', 'two'])
        mock_scheduler = mock.Mock()
        run_collection = mock.create_autospec(jobrun.JobRunCollection)
        self.nodes = mock.create_autospec(node.NodePool)
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory)

        self.config = mock.create_autospec(schema.ConfigJob)
        patcher = mock.patch('tron.core.job.node.NodePoolRepository')
        with patcher as self.mock_node_repo:
            self.job = job.Job(self.config,
                    mock_scheduler,
                    run_collection=run_collection,
                    action_graph=action_graph,
                    node_pool=self.nodes)
            autospec_method(self.job.notify)
            autospec_method(self.job.watch)
            self.job.event = mock.create_autospec(event.EventRecorder)
            yield

    def test__init__(self):
        assert_equal(self.job.config, self.config)

    @mock.patch('tron.core.job.event', autospec=True)
    def test_from_config(self, _mock_event):
        action = mock.Mock(name='first', command='doit', node=None, requires=[])
        job_config = mock.Mock(
            name='ajob',
            node='thenodepool',
            all_nodes=False,
            queueing=True,
            enabled=True,
            run_limit=20,
            actions={action.name: action},
            cleanup_action=None)
        mock_scheduler = mock.Mock()
        parent_context = 'parent_context_token'
        output_path = ["base_path"]
        new_job = job.Job.from_config(job_config, mock_scheduler,
                parent_context, output_path, self.action_runner)

        assert_equal(new_job.scheduler, mock_scheduler)
        assert_equal(new_job.context.next, parent_context)
        self.mock_node_repo.get_instance().get_by_name.assert_called_with(
            job_config.node)
        assert_equal(new_job.enabled, True)
        assert new_job.action_graph

    def test_update_from_job(self):
        action_runner = mock.Mock()
        config = mock.create_autospec(schema.ConfigJob)
        other_job = job.Job(config, 'scheduler', action_runner=action_runner)
        self.job.update_from_job(other_job)
        assert_equal(self.job.name, config.name)
        assert_equal(self.job.scheduler, 'scheduler')
        assert_equal(self.job, other_job)
        self.job.event.ok.assert_called_with('reconfigured')

    def test_status_disabled(self):
        self.job.enabled = False
        assert_equal(self.job.status, self.job.STATUS_DISABLED)

    def test_status_enabled(self):
        def state_in(state):
            return state in [ActionRun.STATE_SCHEDULED, ActionRun.STATE_QUEUED]

        self.job.runs.get_run_by_state = state_in
        assert_equal(self.job.status, self.job.STATUS_ENABLED)

    def test_status_running(self):
        assert_equal(self.job.status, self.job.STATUS_RUNNING)

    def test_status_unknown(self):
        self.job.runs.get_run_by_state.return_value = None
        assert_equal(self.job.status, self.job.STATUS_UNKNOWN)

    def test_state_data(self):
        state_data = self.job.state_data
        assert_equal(state_data['runs'], self.job.runs.state_data)
        assert state_data['enabled']

    def test_restore_state(self):
        run_data = ['one', 'two']
        job_runs = [mock.Mock(), mock.Mock()]
        self.job.runs.restore_state = lambda r, a, o, c, n: job_runs
        state_data = {'enabled': False, 'runs': run_data}
        self.job.output_path = mock.create_autospec(filehandler.OutputPath)

        self.job.restore_state(state_data)

        assert not self.job.enabled
        calls = [mock.call(job_runs[i]) for i in xrange(len(job_runs))]
        self.job.watch.assert_has_calls(calls)
        self.job.event.ok.assert_called_with('restored')

    def test_build_new_runs(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        manual = False
        get_nodes = self.job.node_pool.get_nodes
        mock_nodes = [mock.create_autospec(node.Node) for _ in xrange(2)]
        get_nodes.return_value = mock_nodes
        list(self.job.build_new_runs(run_time, manual=manual))

        self.job.node_pool.get_nodes.assert_called_with(self.config.all_nodes)
        expected = [mock.call(self.job, run_time, mock_node, manual=manual)
                    for mock_node in mock_nodes]
        assert_equal(self.job.runs.build_new_run.mock_calls, expected)
        self.job.watch.assert_called_with(
            self.job.runs.build_new_run.return_value)
        assert_equal(self.job.watch.call_count, len(mock_nodes))

    def test_handler(self):
        self.job.handler(None, jobrun.JobRun.NOTIFY_STATE_CHANGED)
        self.job.notify.assert_called_with(self.job.NOTIFY_STATE_CHANGE)

        self.job.handler(None, jobrun.JobRun.NOTIFY_DONE)
        self.job.notify.assert_called_with(self.job.NOTIFY_RUN_DONE)

    def test__eq__(self):
        other_job = job.Job(self.config, 'scheduler')
        assert not self.job == other_job
        other_job.update_from_job(self.job)
        assert_equal(self.job, other_job)

    def test__ne__(self):
        config = mock.create_autospec(schema.ConfigJob)
        other_job = job.Job(config, 'scheduler')
        assert self.job != other_job
        other_job.update_from_job(self.job)
        assert not self.job != other_job

    def test__eq__true(self):
        action_runner = mock.Mock()
        first = job.Job(self.config, 'scheduler', action_runner=action_runner)
        second = job.Job(self.config, 'scheduler', action_runner=action_runner)
        assert_equal(first, second)

    def test__eq__false(self):
        first = job.Job(self.config, 'scheduler', action_runner=mock.Mock())
        second = job.Job(self.config, 'scheduler', action_runner=mock.Mock())
        assert_not_equal(first, second)


class JobSchedulerTestCase(TestCase):

    @setup_teardown
    def patch_eventloop(self):
        patcher = mock.patch('tron.core.job.eventloop', autospec=True)
        with patcher as self.mock_eventloop:
            yield

    @setup
    def setup_job(self):
        self.scheduler = mock.create_autospec(scheduler.IntervalScheduler)
        run_collection = mock.create_autospec(jobrun.JobRunCollection)
        node_pool = mock.create_autospec(node.NodePool)
        self.config = mock.create_autospec(schema.ConfigJob,
                allow_overlap=False)
        self.job = job.Job(
                self.config,
                self.scheduler,
                run_collection=run_collection,
                node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)

    def test_restore_job_state(self):
        run_collection = mocks.MockJobRunCollection(get_scheduled=lambda: ['a'])
        self.job_scheduler.job = Turtle(runs=run_collection)
        self.job_scheduler._set_callback = Turtle()
        state_data = 'state_data_token'
        self.job_scheduler.restore_state(state_data)
        assert_call(self.job_scheduler.job.restore_state, 0, state_data)
        assert_length(self.job_scheduler._set_callback.calls, 1)
        assert_call(self.job_scheduler._set_callback, 0, 'a')

    def test_disable(self):
        self.job_scheduler.disable()
        assert not self.job.enabled
        self.job.runs.cancel_pending.assert_called_with()

    def test_schedule_reconfigured(self):
        autospec_method(self.job_scheduler.create_and_schedule_runs)
        self.job_scheduler.schedule_reconfigured()
        self.job.runs.remove_pending.assert_called_with()
        self.job_scheduler.create_and_schedule_runs.assert_called_with(
            ignore_last_run_time=True)

    def test_run_job(self):
        autospec_method(self.job_scheduler.schedule)
        self.scheduler.schedule_on_complete = False
        self.job.runs.get_active = lambda n: []
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 1)
        self.job_scheduler.schedule.assert_called_with()

    def test_run_job_shutdown_requested(self):
        self.job_scheduler.shutdown_requested = True
        autospec_method(self.job_scheduler.schedule)
        job_run = Turtle()
        self.job_scheduler.run_job(job_run)
        assert not self.job_scheduler.schedule.mock_calls
        assert_length(job_run.start.calls, 0)
        assert_length(job_run.cancel.calls, 0)

    def test_run_job_job_disabled(self):
        autospec_method(self.job_scheduler.schedule)
        job_run = Turtle()
        self.job.enabled = False
        self.job_scheduler.run_job(job_run)
        assert not self.job_scheduler.schedule.mock_calls
        assert_length(job_run.start.calls, 0)
        assert_length(job_run.cancel.calls, 1)

    def test_run_job_cancelled(self):
        autospec_method(self.job_scheduler.schedule)
        job_run = Turtle(is_scheduled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 0)
        self.job_scheduler.schedule.assert_called_with()

    def test_run_job_already_running_queuing(self):
        autospec_method(self.job_scheduler.schedule)
        self.job.runs.get_active = lambda s: [Turtle()]
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 0)
        assert_length(job_run.queue.calls, 1)
        assert not self.job_scheduler.schedule.mock_calls

    def test_run_job_already_running_cancel(self):
        autospec_method(self.job_scheduler.schedule)
        self.job.runs.get_active.return_value = [mock.Mock]
        self.job.runs.get_active = lambda s: [Turtle()]
        self.config.queueing = False
        job_run = mock.Mock(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert not job_run.start.mock_calls
        job_run.cancel.assert_called_with()
        self.job_scheduler.schedule.assert_called_with()

    def test_run_job_already_running_allow_overlap(self):
        self.job_scheduler.schedule = mock.Mock()
        self.job.runs.get_active = lambda s: [mock.Mock()]
        self.job.allow_overlap = True
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        job_run.start.assert_called_with()

    def test_run_job_has_starting_queueing(self):
        autospec_method(self.job_scheduler.schedule)
        self.job.runs.get_active.return_value = [mock.Mock]
        job_run = mock.Mock(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert not job_run.start.mock_calls
        job_run.queue.assert_called_with()
        assert not self.job_scheduler.schedule.mock_calls

    def test_run_job_schedule_on_complete(self):
        autospec_method(self.job_scheduler.schedule)
        self.scheduler.schedule_on_complete = True
        self.job.runs.get_active = lambda s: []
        job_run = Turtle(is_cancelled=False)
        self.job_scheduler.run_job(job_run)
        assert_length(job_run.start.calls, 1)
        assert not self.job_scheduler.schedule.mock_calls

class JobSchedulerGetRunsToScheduleTestCase(TestCase):

    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock(has_pending=False)
        node_pool = mock.Mock()
        self.config = mock.create_autospec(schema.ConfigJob)
        self.job = job.Job(
            self.config,
            self.scheduler,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)
        self.job.runs.get_pending.return_value = False
        self.scheduler.queue_overlapping = True

    def test_get_runs_to_schedule_has_pending(self):
        self.job.runs.has_pending = True
        job_runs = self.job_scheduler.get_runs_to_schedule(False)
        assert_length(job_runs, 0)

    def test_get_runs_to_schedule_no_pending(self):
        autospec_method(self.job.build_new_runs)
        job_runs = self.job_scheduler.get_runs_to_schedule(False)

        self.job.runs.get_newest.assert_called_with(include_manual=False)
        self.job.scheduler.next_run_time.assert_called_once_with(
            self.job.runs.get_newest.return_value.run_time)
        assert_equal(job_runs, self.job.build_new_runs.return_value)

    def test_get_last_run_time_no_last_run(self):
        self.job.runs.get_newest.return_value = None
        assert not self.job_scheduler._get_last_run_time(False)

    def test_get_last_run_time(self):
        last_run = self.job_scheduler._get_last_run_time(False)
        assert_equal(last_run, self.job.runs.get_newest.return_value.run_time)

    def test_get_last_run_time_ignore_last(self):
        assert not self.job_scheduler._get_last_run_time(True)


class JobSchedulerManualStartTestCase(testingutils.MockTimeTestCase):

    now = datetime.datetime.now()

    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock()
        node_pool = mock.Mock()
        self.config = mock.create_autospec(schema.ConfigJob)
        self.job = job.Job(
            self.config,
            self.scheduler,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)
        self.manual_run = mock.Mock()
        self.job.build_new_runs = mock.Mock(return_value=[self.manual_run])

    def test_manual_start(self):
        manual_runs = self.job_scheduler.manual_start()

        self.job.build_new_runs.assert_called_with(self.now, manual=True)
        assert_length(manual_runs, 1)
        self.manual_run.start.assert_called_once_with()

    def test_manual_start_with_run_time(self):
        run_time = datetime.datetime(2012, 3, 14, 15, 9, 26)
        manual_runs = self.job_scheduler.manual_start(run_time)

        self.job.build_new_runs.assert_called_with(run_time, manual=True)
        assert_length(manual_runs, 1)
        self.manual_run.start.assert_called_once_with()


class JobSchedulerScheduleTestCase(TestCase):

    @setup
    def setup_job(self):
        self.scheduler = mock.Mock()
        run_collection = mock.Mock(has_pending=False)
        node_pool = mock.Mock()
        self.config = mock.create_autospec(schema.ConfigJob)
        self.job = job.Job(
            self.config,
            self.scheduler,
            run_collection=run_collection,
            node_pool=node_pool,
        )
        self.job_scheduler = job.JobScheduler(self.job)

    @setup_teardown
    def mock_eventloop(self):
        patcher = mock.patch('tron.core.job.eventloop', autospec=True)
        with patcher as self.mock_eventloop:
            yield

    @teardown
    def teardown_job(self):
        event.EventManager.reset()

    def test_enable(self):
        autospec_method(self.job_scheduler.create_and_schedule_runs)
        self.job.enabled = False
        self.job_scheduler.enable()
        assert self.job.enabled
        self.job_scheduler.create_and_schedule_runs.assert_called_with(
            ignore_last_run_time=True)

    def test_enable_noop(self):
        self.job.enalbed = True
        self.job_scheduler.enable()
        assert self.job.enabled
        assert_length(self.mock_eventloop.call_later.mock_calls, 0)

    def test_schedule(self):
        autospec_method(self.job_scheduler.create_and_schedule_runs)
        self.job_scheduler.schedule()
        self.job_scheduler.create_and_schedule_runs.assert_called_with()

    def test_schedule_disabled_job(self):
        self.job.enabled = False
        autospec_method(self.job_scheduler.create_and_schedule_runs)
        self.job_scheduler.schedule()
        assert not self.job_scheduler.create_and_schedule_runs.mock_calls

    def test_handle_job_events_schedule_on_complete(self):
        autospec_method(self.job_scheduler.schedule)
        self.job.scheduler.schedule_on_complete = True
        self.job_scheduler.handle_job_events(self.job, job.Job.NOTIFY_RUN_DONE)
        self.job_scheduler.schedule.assert_called_with()

    def test_handler_unknown_event(self):
        self.job_scheduler.handler(self.job, 'some_other_event')
        self.job.runs.get_runs_by_state.assert_not_called()

    def test_handler_no_queued(self):
        autospec_method(self.job_scheduler.run_job)
        autospec_method(self.job_scheduler.schedule)
        self.job.runs.get_first_queued.return_value = []
        self.job_scheduler.handler(self.job, job.Job.NOTIFY_RUN_DONE)
        assert not self.mock_eventloop.call_later.mock_calls
        self.job_scheduler.schedule.assert_called_with()


class JobSchedulerFactoryTestCase(TestCase):

    @setup
    def setup_factory(self):
        self.context = mock.Mock()
        self.output_stream_dir = mock.Mock()
        self.time_zone = mock.Mock()
        self.action_runner = mock.create_autospec(
            actioncommand.SubprocessActionRunnerFactory)
        self.factory = job.JobSchedulerFactory(
            self.context, self.output_stream_dir, self.time_zone, self.action_runner)

    def test_build(self):
        config = mock.Mock()
        with mock.patch('tron.core.job.Job', autospec=True) as mock_job:
            job_scheduler = self.factory.build(config)
            args, _ = mock_job.from_config.call_args
            job_config, scheduler, context, output_path, action_runner = args
            assert_equal(job_config, config)
            assert_equal(job_scheduler.get_job(), mock_job.from_config.return_value)
            assert_equal(context, self.context)
            assert_equal(output_path.base, self.output_stream_dir)
            assert_equal(action_runner, self.action_runner)


class JobCollectionTestCase(TestCase):

    @setup
    def setup_collection(self):
        self.collection = job.JobCollection()

    def test_load_from_config(self):
        autospec_method(self.collection.jobs.filter_by_name)
        autospec_method(self.collection.add)
        factory = mock.create_autospec(job.JobSchedulerFactory)
        job_configs = {'a': mock.Mock(), 'b': mock.Mock()}
        result = self.collection.load_from_config(job_configs, factory, True)
        result = list(result)
        self.collection.jobs.filter_by_name.assert_called_with(job_configs)
        expected_calls = [mock.call(v) for v in job_configs.itervalues()]
        assert_mock_calls(expected_calls, factory.build.mock_calls)
        assert_length(self.collection.add.mock_calls, len(job_configs) * 2)
        assert_length(result, len(job_configs))
        job_schedulers = [call[1][0] for call in self.collection.add.mock_calls[::2]]
        for job_scheduler in job_schedulers:
            job_scheduler.schedule.assert_called_with()
            job_scheduler.get_job.assert_called_with()

    def test_update(self):
        mock_scheduler = mock.create_autospec(job.JobScheduler)
        existing_scheduler = mock.create_autospec(job.JobScheduler)
        autospec_method(self.collection.get_by_name, return_value=existing_scheduler)
        assert self.collection.update(mock_scheduler)
        self.collection.get_by_name.assert_called_with(mock_scheduler.get_name())
        existing_scheduler.get_job().update_from_job.assert_called_with(
            mock_scheduler.get_job.return_value)
        existing_scheduler.schedule_reconfigured.assert_called_with()


if __name__ == '__main__':
    run()
