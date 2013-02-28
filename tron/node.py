import functools
import logging
import itertools
import random
from twisted.conch.client.knownhosts import KnownHostsFile

from twisted.conch.client.options import ConchOptions
from twisted.internet import protocol, defer, reactor
from twisted.python import failure
from twisted.python.filepath import FilePath

from tron import ssh, eventloop
from tron.utils import twistedutils, collections, state


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
        self.channel    = None

    def start(self, channel):
        # TODO: how is channel __str__ ?
        log.info("Run %s started on %s", self.run.id, self.channel)
        channel.start_defer = None
        assert self.state.transition('start')
        run.started()

    def create_channel(self, connection):
        assert self.state.transition('connect')

        start_defer = eventloop.build_defer(self.start, self._run_start_error)
        start_defer.setTimeout(RUN_START_TIMEOUT, start_defer.cancel)

        exit_defer = eventloop.build_defer(self._channel_complete,
                                           self._channel_complete_unknown)

        channel = ssh.build_channel(connection, self.run, start_defer, exit_defer)
        self.channel = channel
        return channel

    def _run_start_error(self, result, run):
        """We failed to even run the command due to communication difficulties
        """
        log.error("Error running %s: %s", run.id, str(result))
        # TODO: better failure
        self._fail_run(failure.Failure(
            exc_value=ConnectError("Connection to %s failed" % self.hostname)))

    def _channel_complete(self, channel):
        log.info("Run %s has completed with %r", self.run.id, channel.exit_status)
        assert self.state.transition('done')
        # TODO: watch and trigger cleanup(), or is this redundent because we
        # already have a deferred?
        self.notify(self.COMPLETE)

        self.run.exited(channel.exit_status)
        self.deferred.callback(channel.exit_status)

    def _channel_complete_unknown(self, result):
        """Channel closed on a running process without a proper exit """
        log.error("Failure waiting on channel completion: %s", str(result))
        # TODO: Better failure, why custom error instead of just using fail_run directly ?
        self._fail_run(failure.Failure(exc_value=ResultError()))

    def _fail_run(self, result):
        """The run failed."""
        log.debug("Run %s has failed", run.id)
        self.notify(self.COMPLETE)
        self.run.exited(None)

        log.info("Calling fail_run callbacks")
        self.deferred.errback(result)


class RunCollection(object):

    def __init__(self):
        self.runs = {}

    def add(self, action_command):
        if action_command.id in self.run:
            raise Error("Run %s already running !?!" % action_command.id)

        self.runs[action_command.id] = ChannelState(action_command)
        return self.runs[action_command.id]

    def remove(self, action_command):
        # TODO: why set to None before deleting it?
        self.runs[action_command.id].channel = None
        del self.runs[run.id]

    def get(self, run):
        return self.runs.get(run.id)

    def __len__(self):
        return len(self.runs)

    def __iter__(self):
        return self.runs.iteritems()


class NodeConnection(object):

    def __init__(self, connection, hostname):
        # TODO: logging only?
        self.hostname = hostname
        self.connection = connection
        self.idle_timer = eventloop.NullCallback
        # TODO: can these be properties
        self.is_connecting = False
        self.is_connected = False
        self.connection_defer = None

    def idle_timeout(self):
        log.info("Connection to %s idle for %d secs. Closing.",
                     self.hostname, IDLE_CONNECTION_TIMEOUT)
        self.connection.transport.loseConnection()

    def cancel_idle(self):
        if self.idle_timer.active():
            self.idle_timer.cancel()

    def start_idle(self):
        self.idle_timer = eventloop.call_later(IDLE_CONNECTION_TIMEOUT, self.idle_timeout)



class NullNodeConnection(object):
    is_connected = False
    is_connecting = False

    @staticmethod
    def cancel_idle():
        pass


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

    def build(self):
        client_creator = protocol.ClientCreator(reactor,
            ssh.ClientTransport, self.username, self.conch_options, self.public_key)
        create_defer = client_creator.connectTCP(self.hostname, 22)

        # We're going to create a deferred, returned to the caller, that will
        # be called back when we have an established, secure connection ready
        # for opening channels. The value will be this instance of node.
        connect_defer = defer.Deferred()
        twistedutils.defer_timeout(connect_defer, CONNECT_TIMEOUT)

        def on_service_started(connection):
            # Booyah, time to start doing stuff
            self.connection = connection
            self.connection_defer = None

            connect_defer.callback(self)
            return connection

        def on_connection_secure(connection):
            # We have a connection, but it might not be fully ready....
            connection.service_start_defer = defer.Deferred()
            connection.service_stop_defer = defer.Deferred()

            connection.service_start_defer.addCallback(on_service_started)
            connection.service_stop_defer.addCallback(self._service_stopped)
            return connection

        def on_transport_create(transport):
            transport.connection_defer = defer.Deferred()
            transport.connection_defer.addCallback(on_connection_secure)
            return transport

        def on_transport_fail(fail):
            log.warning("Cannot connect to %s", self.hostname)

        create_defer.addCallback(on_transport_create)
        create_defer.addErrback(on_transport_fail)

        return connect_defer




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
        self.run_states = RunCollection()

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

    # TODO: combine with run ?
    def submit_command(self, command):
        """Submit an ActionCommand to be run on this node. Optionally provide
        an error callback which will be called on error.
        """
        deferred = self.run(command)
        deferred.addErrback(command.handle_errback)
        return deferred

    def run(self, run):
        """Execute the specified run

        A run consists of a very specific set of interfaces which allow us to
        execute a command on this remote machine and return results.
        """
        log.info("Running %s for %s on %s", run.command, run.id, self.hostname)
        run_state = self.run_states.add(run)
        self.connection.cancel_idle()
        self.start_runner(run)
        return run_state.deferred

    def start_runner(self, run):
        fudge_factor = determine_fudge_factor(len(self.run_states))
        if not fudge_factor:
            return self._do_run(run)
        log.info("Delaying execution of %s for %.2f secs", run.id, fudge_factor)
        eventloop.call_later(fudge_factor, self._do_run, run)

    def _do_run(self, run):
        """Finish starting to execute a run. This step may have been delayed.  """
        if not self.connection.connected:
            self._connect_then_run(run)
        else:
            self._open_channel(run)

    def _connect_then_run(self, run):
        # Have we started the connection process ?
        if self.connection_defer is None:
            self.connection_defer = self._connect()

        def call_open_channel(arg):
            self._open_channel(run)
            return arg

        def connect_fail(result):
            log.warning("Cannot run %s, Failed to connect to %s",
                        run.id, self.hostname)
            self.connection_defer = None
            self._fail_run(run, failure.Failure(
                exc_value=ConnectError("Connection to %s failed" %
                                       self.hostname)))

        self.connection_defer.addCallback(call_open_channel)
        self.connection_defer.addErrback(connect_fail)

    def _open_channel(self, run):
        assert self.connection.connected
        run_state = self.run_state.get(run)
        channel = run_state.create_channel(self.connection)
        self.connection.openChannel(channel)

    def _cleanup(self, run):
        self.run_state.remove(run)
        if not self.run_states:
            self.connection.start_idle()

    def _service_stopped(self, connection):
        """Called when the SSH service has disconnected fully.

        We should be in a state where we know there are no runs in progress
        because all the SSH channels should have disconnected them.
        """
        assert self.connection is connection.connection
        self.connection = NullNodeConnection

        log.info("Service to %s stopped", self.hostname)

        for run_id, run in self.run_states:
            if run.state == RUN_STATE_CONNECTING:
                # Now we can trigger a reconnect and re-start any waiting runs.
                self._connect_then_run(run)
            elif run.state == RUN_STATE_RUNNING:
                self._fail_run(run, None)
            elif run.state == RUN_STATE_STARTING:
                if run.channel and run.channel.start_defer is not None:

                    # This means our run IS still waiting to start. There
                    # should be an outstanding timeout sitting on this guy as
                    # well. We'll just short circut it.
                    twistedutils.defer_timeout(run.channel.start_defer, 0)
                else:
                    # Doesn't seem like this should ever happen.
                    log.warning("Run %r caught in starting state, but"
                                " start_defer is over.", run_id)
                    self._fail_run(run, None)
            else:
                # Service ended. The open channels should know how to handle
                # this (and cleanup) themselves, so if there should not be any
                # runs except those waiting to connect
                raise Error("Run %s in state %s when service stopped",
                            run_id, run.state)


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
