import functools
import logging
import itertools
import random

from twisted.conch.client.knownhosts import KnownHostsFile
from twisted.conch.client.options import ConchOptions
from twisted.internet import protocol, defer
from twisted.python import failure
from twisted.python.filepath import FilePath

from tron import ssh, eventloop
from tron.utils import collections, state


log = logging.getLogger(__name__)


# TODO: make these configurable
# We should also only wait a certain amount of time for a connection to be
# established.
CONNECT_TIMEOUT = 30

IDLE_CONNECTION_TIMEOUT = 3600

# We should also only wait a certain amount of time for a new channel to be
# established when we already have an open connection.  This timeout will
# usually get triggered prior to even a TCP timeout, so essentially it's our
# shortcut to discovering the connection died.
RUN_START_TIMEOUT = 20


class Error(Exception):
    pass


class ConnectError(Error):
    """There was a problem connecting, run was never started"""
    pass


class ResultError(Error):
    """There was a problem retrieving the result from this run

    We did try to execute the command, but we don't know if it succeeded or
    failed.
    """
    pass


class NodePoolRepository(object):
    """A Singleton to store Node and NodePool objects."""

    _instance = None

    def __init__(self):
        if self._instance is not None:
            raise ValueError("NodePoolRepository is already instantiated.")
        super(NodePoolRepository, self).__init__()
        self.nodes = collections.MappingCollection('nodes')
        self.pools = collections.MappingCollection('pools')

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def filter_by_name(self, node_configs, node_pool_configs):
        self.nodes.filter_by_name(node_configs)
        self.pools.filter_by_name(node_configs.keys() + node_pool_configs.keys())

    @classmethod
    def update_from_config(cls, node_configs, node_pool_configs, ssh_config):
        instance = cls.get_instance()
        ssh_options = build_ssh_options_from_config(ssh_config)
        known_hosts = KnownHosts.from_path(ssh_config.known_hosts_file)
        instance.filter_by_name(node_configs, node_pool_configs)

        for config in node_configs.itervalues():
            pub_key = known_hosts.get_public_key(config.hostname)
            instance.add_node(Node.from_config(config, ssh_options, pub_key))

        for config in node_pool_configs.itervalues():
            nodes = instance._get_nodes_by_name(config.nodes)
            pool  = NodePool.from_config(config, nodes)
            instance.pools.add(pool, instance.pools.remove)

    def add_node(self, node):
        self.nodes.add(node, self.nodes.remove)
        self.pools.add(NodePool.from_node(node), self.pools.remove)

    def get_node(self, node_name, default=None):
        return self.nodes.get(node_name, default)

    def __contains__(self, node):
        return node.get_name() in self.pools

    def get_by_name(self, name, default=None):
        return self.pools.get(name, default)

    def _get_nodes_by_name(self, names):
        return [self.nodes[name] for name in names]

    def clear(self):
        self.nodes.clear()
        self.pools.clear()


class NodePool(object):
    """A pool of Node objects."""
    def __init__(self, nodes, name):
        self.nodes      = nodes
        self.disabled   =  False
        self.name       = name or '_'.join(n.get_name() for n in nodes)
        self.iter       = itertools.cycle(self.nodes)

    @classmethod
    def from_config(cls, node_pool_config, nodes):
        return cls(nodes, node_pool_config.name)

    @classmethod
    def from_node(cls, node):
        return cls([node], node.get_name())

    def __eq__(self, other):
        return isinstance(other, NodePool) and self.nodes == other.nodes

    def __ne__(self, other):
        return not self == other

    def get_name(self):
        return self.name

    def next(self):
        """Return a random node from the pool."""
        return random.choice(self.nodes)

    def next_round_robin(self):
        """Return the next node cycling in a consistent order."""
        return self.iter.next()

    def disable(self):
        """Required for MappingCollection.Item interface."""
        self.disabled = True

    def get_by_hostname(self, hostname):
        for node in self.nodes:
            if node.hostname == hostname:
                return node

    def __str__(self):
        return "NodePool:%s" % self.name


class KnownHosts(KnownHostsFile):
    """Lookup host key for a hostname."""

    @classmethod
    def from_path(cls, file_path):
        if not file_path:
            return cls(None)
        return cls.fromPath(FilePath(file_path))

    def get_public_key(self, hostname):
        for entry in self._entries:
            if entry.matchesHost(hostname):
                return entry.publicKey
        log.warn("Missing host key for: %s", hostname)


def determine_fudge_factor(count, min_count=4):
    """Return a random number. """
    fudge_factor = max(0.0, count - min_count)
    return random.random() * float(fudge_factor)


class ChannelStateMachine(state.StateMachine):
    COMPLETE    = state.NamedEventState('complete')
    RUNNING     = state.NamedEventState('running',      done=COMPLETE)
    STARTING    = state.NamedEventState('starting',     start=RUNNING)
    CONNECTING  = state.NamedEventState('connecting',   connect=STARTING)

    initial_state = CONNECTING


class ChannelState(object):

    def __init__(self, action_command):
        self.run        = action_command
        self.state      = ChannelStateMachine()
        self.deferred   = defer.Deferred()
        self.deferred.addErrback(action_command.handle_errback)
        self.channel    = None

    def get_state(self):
        return self.state.state

    def get_deferred(self):
        return self.deferred

    def start(self, channel):
        # TODO: how is channel __str__ ?
        log.info("Run %s started on %s", self.run.get_id(), self.channel)
        # TODO: removed - channel.start_defer = None
        assert self.state.transition('start')
        self.run.started()
        return channel

    def create_channel(self, connection):
        assert self.state.transition('connect')

        start_defer = eventloop.build_defer(self.start, self.start_error)
        eventloop.defer_timeout(RUN_START_TIMEOUT, start_defer)
        exit_defer = eventloop.build_defer(self._channel_complete,
                                           self._channel_complete_unknown)

        self.channel = ssh.build_channel(
            connection, self.run, start_defer, exit_defer)
        connection.openChannel(self.channel)
        return connection

    def start_error(self, reason):
        """We failed to even run the command due to communication difficulties
        """
        log.error("Error running %s: %s", self.run, reason)
        msg = "Channel start error %s: %s" % (self.channel, reason)
        self.fail_run(failure.Failure(ConnectError(msg)))

    def _channel_complete(self, channel):
        log.info("Run %s has completed with %r", self.run.id, channel.exit_status)
        assert self.state.transition('done')
        self.run.exited(channel.exit_status)
        self.deferred.callback(channel.exit_status)

    def _channel_complete_unknown(self, result):
        """Channel closed on a running process without a proper exit """
        log.error("Failure waiting on channel completion: %s", str(result))
        # TODO: Better failure, why custom error instead of just using fail_run directly ?
        self.fail_run(failure.Failure(exc_value=ResultError()))

    def fail_run(self, result):
        """The run failed."""
        log.warn("%s failed with: %s", self.run, result)
        self.run.write_stderr(str(result))
        self.run.exited(None)
        self.run.done()
        self.deferred.errback(result)


class ChannelStateCollection(object):

    def __init__(self):
        self.channel_states = {}

    def add(self, action_command):
        if action_command.get_id() in self.channel_states:
            raise Error("Run %s already running !?!" % action_command.get_id())

        self.channel_states[action_command.get_id()] = ChannelState(action_command)
        return self.channel_states[action_command.get_id()]

    def remove(self, action_command):
        del self.channel_states[action_command.get_id()]

    def get(self, action_command):
        return self.channel_states.get(action_command.get_id())

    def __len__(self):
        return len(self.channel_states)

    def __iter__(self):
        return self.channel_states.iteritems()


class NodeConnection(object):

    def __init__(self, hostname):
        # TODO: logging only?
        self.hostname = hostname
        self.idle_timer = eventloop.NullCallback
        self.deferred = defer.Deferred()
        self.connection = None

    @property
    def is_connecting(self):
        return not bool(self.connection)

    @property
    def is_connected(self):
        return bool(self.connection)

    def idle_timeout(self):
        log.info("Connection to %s idle for %d secs. Closing.",
                     self.hostname, IDLE_CONNECTION_TIMEOUT)
        self.connection.transport.loseConnection()

    def cancel_idle(self):
        if self.idle_timer.active():
            self.idle_timer.cancel()

    def start_idle(self):
        self.idle_timer = eventloop.call_later(IDLE_CONNECTION_TIMEOUT, self.idle_timeout)

    def get_deferred(self):
        return self.deferred

    def get_connection(self):
        return self.connection

    def set_connection(self, connection):
        self.connection = connection
        self.deferred.callback(connection)
        return connection


class NullNodeConnection(object):
    is_connected = False
    is_connecting = False

    @staticmethod
    def cancel_idle(): pass

    @staticmethod
    def start_idle(): pass

    get_connection = None


class ConnectionFactory(object):
    """Build a connection.  This is complicated because we have to deal with a
    few different steps before our connection is really available for us:
        1. Transport is created (our client creator does this)
        2. Our transport is secure, and we can create our connection
        3. The connection service is started, so we can use it
    """

    def __init__(self, username, hostname, conch_options, public_key):
        self.hostname = hostname
        self.username = username
        self.conch_options = conch_options
        self.public_key = public_key

    @classmethod
    def from_node(cls, node):
        factory = cls(
                node.username, node.hostname, node.conch_options, node.pub_key)
        return factory.build()

    def build(self):
        node_connection = NodeConnection(self.hostname)
        client_creator  = protocol.ClientCreator(eventloop.get_reactor(),
                            ssh.ClientTransport,
                            self.username,
                            self.conch_options,
                            self.public_key)


        def on_connection_secure(connection):
            connection.service_start_defer.addCallback(node_connection.set_connection)
            return connection

        def on_transport_create(transport):
            transport.connection_defer.addCallback(on_connection_secure)
            return transport

        def on_transport_fail(fail):
            log.warning("Cannot connect to %s", self.hostname)
            node_connection.get_deferred().errback(fail)

        # TODO: add port to config
        # TODO: test timeout - eventloop.defer_timeout(CONNECT_TIMEOUT, connect_defer)
        create_defer = client_creator.connectTCP(self.hostname, 22, CONNECT_TIMEOUT)
        create_defer.addCallbacks(on_transport_create, on_transport_fail)
        return node_connection


def get_runner(pending_count):
    fudge_factor = determine_fudge_factor(pending_count)
    if not fudge_factor:
        return lambda func, *args: func(*args)
    return functools.partial(eventloop.call_later, fudge_factor)


class Node(object):
    """A node is tron's interface to communicating with an actual machine.
    """

    def __init__(self, hostname, ssh_options, username=None, name=None, pub_key=None):
        self.hostname = hostname
        self.username = username
        self.name = name or hostname
        self.conch_options = ssh_options
        self.connection = NullNodeConnection

        # Map of run id to instance of ChannelState
        self.run_channels = ChannelStateCollection()

        self.disabled = False
        self.pub_key = pub_key

    @classmethod
    def from_config(cls, node_config, ssh_options, pub_key):
        return cls(
            node_config.hostname,
            ssh_options,
            username=node_config.username,
            name=node_config.name,
            pub_key=pub_key)

    def submit_command(self, action_command):
        """Submit an ActionCommand to be run on this node."""
        log.info("Running %s on %s", action_command, self)
        run_channel = self.run_channels.add(action_command)
        self.connection.cancel_idle()

        def cleanup_callback(result):
            self._cleanup(action_command)

        deferred = run_channel.get_deferred()
        deferred.addBoth(cleanup_callback)
        get_runner(len(self.run_channels))(self._do_run, run_channel)
        return deferred

    def _do_run(self, run_channel):
        """Finish starting to execute a run. This step may have been delayed."""
        if not self.connection.is_connected:
            self._connect_then_run(run_channel)
        else:
            run_channel.create_channel(self.connection.get_connection())

    def _connect_then_run(self, run_channel):
        assert not self.connection.is_connected
        if not self.connection.is_connecting:
            self.connection = ConnectionFactory.from_node(self)

        def connect_fail(result):
            msg = "Cannot run %s, Failed to connect to %s: %s"
            log.warn(msg, run_channel, self.hostname, result)
            self.connection = NullNodeConnection
            # TODO: error ConnectError("Connection to %s failed" % self.hostname) ?
            run_channel.fail_run(result)
            return result

        def add_service_stopped(connection):
            connection.service_stop_defer.addCallback(self._service_stopped)
            return connection

        deferred = self.connection.get_deferred()
        deferred.addCallbacks(run_channel.create_channel, connect_fail)
        # TODO: test this is called
        deferred.addCallback(add_service_stopped)

    def _cleanup(self, action_command):
        self.run_channels.remove(action_command)
        if not self.run_channels:
            self.connection.start_idle()

    def _service_stopped(self, connection):
        """Called when the SSH service has disconnected fully.

        We should be in a state where we know there are no runs in progress
        because all the SSH channels should have disconnected them.
        """
        assert self.connection is connection.connection
        log.info("Service to %s stopped", self.hostname)
        self.connection = NullNodeConnection

        for run_id, run_channel in self.run_channels:
            if run_channel.get_state() == ChannelStateMachine.CONNECTING:
                # Reconnect
                self._connect_then_run(run_channel)
                continue

            if run_channel.get_state() == ChannelStateMachine.RUNNING:
                run_channel.fail_run(None)
                continue

            if run_channel.get_state() == ChannelStateMachine.STARTING:
                msg = '%s service stopped.' % self
                start_failure = failure.Failure(ConnectError(msg))
                run_channel.start_error(start_failure)
                continue

            # All completed runs should already be removed from run_channels
            msg = "%s in state %s when service stopped"
            raise Error(msg % (run_channel, run_channel.get_state))

    def __str__(self):
        return "Node:%s@%s" % (self.username or "<default>", self.hostname)

    __repr__ = __str__

    def get_name(self):
        return self.name

    def disable(self):
        """Required for MappingCollection.Item interface."""
        self.disabled = True

    # TODO: wrap conch_options for equality
    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return (self.hostname == other.hostname and
                self.name == other.name and
                self.conch_options == other.conch_options and
                self.pub_key == other.pub_key)


def build_ssh_options_from_config(ssh_options_config):
    ssh_options = ConchOptions()
    ssh_options['agent']        = ssh_options_config.agent
    ssh_options['noagent']      = not ssh_options_config.agent

    for file_name in ssh_options_config.identities:
        ssh_options.opt_identity(file_name)

    return ssh_options
