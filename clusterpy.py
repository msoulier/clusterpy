#!/usr/bin/python3

import sys
import os
import argparse
from enum import Enum
import threading
import time
import socket
import logging
import signal
import select
import queue
import json
import datetime
import time
from typing import TypeVar, Tuple, List

log = logging.getLogger("clusterpy")

unicode = True
shutdown_asap = threading.Event()

msgdelim = "\r\n\r\n"
msgdelimb = b"\r\n\r\n"

VERSION = 0.1
MAX_QUEUESIZE = 100000
SHORT_SLEEPTIME = 2
LONG_SLEEPTIME = 10
BUFSIZE = 1024 * 1024
TIMEOUT = 3
GOOD = "\U0001f7e2"
BAD = "\U0001f7e5"
if not unicode:
    GOOD = "↑"
    BAD = "↓"

ConnectionHandlerT = TypeVar("ConnectionHandlerT", bound="ConnectionHandler")

def setup_native_logger(level):
    """Call this if you want logs and you're not setting up your own."""
    formatter = logging.Formatter('%(asctime)s: [%(levelname)s] %(threadName)s %(name)s:%(lineno)s %(message)s', datefmt="%m-%d-%YT%I:%M:%S%z")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    log.setLevel(level)
    log.addHandler(handler)

def replace_logger(newlogger: logging.Logger):
    """Replace this module's default logger with your own."""
    global log
    while len(log.handlers) > 0:
        log.handlers.pop()

    log = newlogger

class ThreadTracker(object):
    def __init__(self):
        self.threads = {}

    def track_thread(self, sig, thread):
        if sig in self.threads:
            raise AssertionError(f"duplicate sig: {sig}")
        log.debug("tracking thread: %s", sig)
        self.threads[sig] = thread

    def untrack_thread(self, sig):
        log.debug("untracking thread: %s", sig)
        if sig in self.threads:
            del self.threads[sig]
        else:
            pass

    def status_report(self):
        for sig in self.threads:
            log.info("thread %s alive: %b", sig, self.threads[sig].is_alive())

            if not self.threads[sig].is_alive():
                self.untrack_thread(sig)

thread_tracker = ThreadTracker()

class ClusterMessage(object):
    """Formal cluster messages should be encapsulated in this class."""
    def __init__(self, fromid: str, toid: str, payload: str, broadcasted=False):
        self.fromid = fromid
        self.toid = toid
        self.payload = payload
        self.broadcasted = broadcasted
        self.dtsent = None
        self.dtreceived = None

    def __str__(self):
        s = f"from: {self.fromid} to: {self.toid} bcast: {self.broadcasted} payload: {self.payload}"
        if self.dtsent is not None:
            s += "\nsent: " + str(self.get_dtsent())
        if self.dtreceived is not None:
            s += "\nreceived: " + str(self.get_dtreceived())

        return s

    def tojson(self) -> str:
        msg = {
            "from": self.fromid,
            "to": self.toid,
            "broadcasted": self.broadcasted,
            "payload": self.payload
        }
        if self.dtsent is not None:
            msg["dtsent"] = self.dtsent
        if self.dtreceived is not None:
            msg["dtreceived"] = self.dtreceived

        return json.dumps(msg)

    def fromjson(self):
        assert self.payload is not None
        try:
            j = json.loads(self.payload)
            self.fromid = j["from"]
            self.toid = j["to"]
            self.broadcasted = j["broadcasted"]
            self.payload = j["payload"]
            if "dtsent" in j:
                self.dtsent = j["dtsent"]
            if "dtreceived" in j:
                self.dtreceived = j["dtreceived"]

        except (TypeError, ValueError) as err:
            log.error("fromjson: bad input: %s", err)
            raise

    def set_dtsent(self, dt: datetime.datetime):
        """Set the dtsent timestamp from a datetime object."""
        try:
            self.dtsent = dt.timestamp()

        except (TypeError, ValueError) as err:
            log.error("set_dtsent: bad input: %s", err)

    def get_dtsent(self) -> datetime.datetime:
        """Return the dtsent timestamp as a datetime object."""
        if self.dtsent is not None:
            try:
                dt = datetime.datetime.fromtimestamp(self.dtsent)
                return dt

            except (TypeError, ValueError) as err:
                log.error("get_dtsent: bad input: %s", err)

        return None

    def set_dtreceived(self, dt: datetime.datetime):
        """Set the dtreceived timestamp from a datetime object."""
        try:
            self.dtreceived = dt.timestamp()

        except (TypeError, ValueError) as err:
            log.error("set_dtreceived: bad input: %s", err)

    def get_dtreceived(self) -> datetime.datetime:
        """Return the dtreceived timestamp as a datetime object."""
        if self.dtreceived is not None:
            try:
                dt = datetime.datetime.fromtimestamp(self.dtreceived)
                return dt

            except (TypeError, ValueError) as err:
                log.error("get_dtreceived: bad input: %s", err)

        return None

    def tof(self) -> float:
        """Return the time-of-flight of the message."""
        if self.dtsent is not None and self.dtreceived is not None:
            log.debug("tof: received %s - sent %s", self.dtreceived, self.dtsent)
            delta = self.dtreceived - self.dtsent
            return delta
        else:
            return None

class ConnectionError(RuntimeError):
    pass

class ConnectionState(Enum):
    """INTENT_CONNECT - we intend to initiate the connection
       INTENT_LISTEN - we intend to listen for a connection
       LISTENING - we are bound and listening
       CONNECTEDA - we are connected, initiated from node_a
       CONNECTEDB - we are connected, initiated from node_b
       UNKNOWN - either don't know the state, or don't know the node"""
    INTENT_CONNECT = 1
    INTENT_LISTEN = 2
    LISTENING = 3
    CONNECTEDA = 4
    CONNECTEDB = 5
    UNKNOWN = 6

class ClusterNode(object):
    def __init__(self, nodeid, address, listenport):
        assert nodeid is not None
        assert address is not None
        assert listenport is not None

        self.address = address
        self.listenport = int(listenport)
        self.nodeid = nodeid
        self.connections: List[ConnectionHandlerT] = []
        self.listen_threads = []
        self.connect_threads = []
        self.success_callbacks = []
        self.error_callbacks = []

    def __gt__(self, other):
        if str(self) > str(other):
            return True
        else:
            return False

    def __str__(self):
        return "ClusterNode: %s" % (self.nodeid)

    def add_success_callback(self, callback):
        """Add a success callback to all connections."""
        self.success_callbacks.append(callback)
        for conn in self.connections:
            conn.add_success_callback(callback)

    def add_error_callback(self, callback):
        """Add an error callback to all connections."""
        self.error_callbacks.append(callback)
        for conn in self.connections:
            conn.add_error_callback(callback)

    def sig(self):
        return f"{self.nodeid}-{self.address}:{self.listenport}"

    def start_networking(self):
        """Start the networking threads."""
        if len(self.listen_threads) > 0:
            self.listen_threads[0].start()

        for connect_thread in self.connect_threads:
            connect_thread.start()

    def shutdown_threads(self):
        shutdown_asap.set()
        log.debug("setting SIGALRM for 1s")
        signal.alarm(1)
        if len(self.listen_threads) > 0:
            for listen_thread in self.listen_threads:
                log.info("stopping listen thread")
                listen_thread.shutdown()
                listen_thread.join()

        for connect_thread in self.connect_threads:
            log.info("stopping connect thread %s", connect_thread)
            connect_thread.shutdown()
            connect_thread.join()

    def add_connection(self, conn: ConnectionHandlerT):
        log.debug("adding connection %s to %s", conn, self)
        self.connections.append(conn)
        for callback in self.success_callbacks:
            conn.add_success_callback(callback)
        for callback in self.error_callbacks:
            conn.add_error_callback(callback)

        if conn.state == ConnectionState.INTENT_CONNECT:
            connthread = ConnectionThread(conn, name=conn.sig())
            self.connect_threads.append(connthread)
            thread_tracker.track_thread(conn.sig(), connthread)
            log.debug("added an INTENT_CONNECT thread")

        elif conn.state == ConnectionState.INTENT_LISTEN:
            connthread = ConnectionThread(conn, name=conn.sig())
            if len(self.listen_threads) > 0:
                log.debug("discarding redundant listening thread")
            else:
                self.listen_threads.append(connthread)
                thread_tracker.track_thread(conn.sig(), connthread)
                log.debug("added an INTENT_LISTEN thread")

        elif conn.state == ConnectionState.CONNECTEDB:
            connthread = ConnectionThread(conn, name=conn.sig())
            self.listen_threads.append(connthread)
            log.debug("added a CONNECTEDB thread")
            # This type of thread is added after we're up and running, so start
            # it now.
            thread_tracker.track_thread(conn.sig(), connthread)
            connthread.start()
            # and call any success callbacks on it, since this is a new
            # connection that has successfully come up
            conn.success()

        else:
            raise AssertionError("wrong state for add_connection: %s", conn.state)

class ConnectionWritingThread(threading.Thread):
    def __init__(self, conn: ConnectionHandlerT, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("ConnectionWritingThread.ctor()")
        self.conn = conn

    def run(self):
        log.debug("ConnectionWritingThread starting")
        while not shutdown_asap.is_set():
            if ( self.conn.state != ConnectionState.CONNECTEDB and 
                 self.conn.state != ConnectionState.CONNECTEDA ):
                time.sleep(SHORT_SLEEPTIME)
                continue
            try:
                msg = self.conn.sending_queue.get(block=True, timeout=TIMEOUT)
                log.debug("found message on sending queue: %s", msg)
                self.conn.send(msg)
            except queue.Empty as err:
                continue
            except ConnectionError as err:
                log.error("connection error: %s", err)
                continue

class ConnectionReadingThread(threading.Thread):
    def __init__(self, conn: ConnectionHandlerT, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("ConnectionReadingThread.ctor()")
        self.conn = conn
        self.partial = None

    def run(self):
        log.debug("ConnectionReadingThread starting")
        while not shutdown_asap.is_set():
            msg = self.conn.receive()
            if msg is not None:
                log.debug("reading thread: received a message: %s", msg)
                self.conn.receiving_queue.put(msg, block=True, timeout=TIMEOUT)
            else:
                log.debug("reading thread: receive returned None")
                time.sleep(SHORT_SLEEPTIME)

class ConnectionThread(threading.Thread):
    def __init__(self, conn: ConnectionHandlerT, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("ConnectionThread.ctor()")
        self.conn = conn
        self.sleeptime_connected = LONG_SLEEPTIME
        self.sleeptime_not_connected = SHORT_SLEEPTIME
        self.read_thread = None
        self.write_thread = None

    def shutdown(self):
        log.debug("joining read thread")
        self.read_thread.join()
        log.debug("joining write thread")
        self.write_thread.join()

    def run(self):
        log.debug("ConnectionThread starting")
        while not shutdown_asap.is_set():
            # Go into listen state, or attempt to connect, or status report
            self.conn.manage()
            if self.conn.sock is None:
                log.debug("waiting for socket")
                time.sleep(SHORT_SLEEPTIME)
                continue

            # Only fully connected handlers need their own threads.
            if ( self.conn.state == ConnectionState.CONNECTEDA or
                 self.conn.state == ConnectionState.CONNECTEDB ):
                if self.read_thread is None or not self.read_thread.is_alive():
                    name = self.conn.sig() + "-reading"
                    if self.read_thread is None:
                        log.info("initializing read thread %s", name)
                    else:
                        log.error("read thread is dead - recreating")
                    self.read_thread = ConnectionReadingThread(self.conn, name=name)
                    self.read_thread.start()

                if self.write_thread is None or not self.write_thread.is_alive():
                    name = self.conn.sig() + "-writing"
                    if self.write_thread is None:
                        log.info("initializing write thread %s", name)
                    else:
                        log.error("write thread is dead - recreating")
                    self.write_thread = ConnectionWritingThread(self.conn, name=name)
                    self.write_thread.start()

            time.sleep(LONG_SLEEPTIME)

class ConnectionManager(object):
    """Manage the connections between nodes and routing of messages."""
    def __init__(self, selfnode: ClusterNode):
        assert selfnode is not None
        self.selfnode = selfnode
        self.nodes = []
        log.debug("set selfnode to %s", self.selfnode)

    def remote_nodeid(self, conn: ConnectionHandlerT):
        """Given the ConnectionHandler conn, return the node id of the remote
        end of the connection."""
        if conn.node_a.nodeid == self.selfnode.nodeid:
            return conn.node_b.nodeid
        elif conn.node_b.nodeid == self.selfnode.nodeid:
            return conn.node_a.nodeid
        else:
            return None

    def msgs_waiting(self) -> bool:
        for conn in self.selfnode.connections:
            if not conn.receiving_queue.empty():
                return True
        return False

    def incoming_messages(self):
        for conn in self.selfnode.connections:
            while not conn.receiving_queue.empty():
                try:
                    msg = conn.receiving_queue.get(block=True, timeout=TIMEOUT)
                    yield msg
                except queue.Empty as err:
                    log.error("expected a message but queue is empty")
                    continue

    def shutdown(self):
        log.info("ConnectionManager.shutdown")
        self.selfnode.shutdown_threads()

    def add_node(self, node: ClusterNode):
        assert node is not None
        self.nodes.append(node)

    def connection_state(self, nodeid: str) -> ConnectionState:
        """Return the connection state of our connection to nodeid."""
        if nodeid == self.selfnode.nodeid:
            log.warning("connection_state: request to connect to selfnode")
            return ConnectionState.CONNECTEDA
        for connection in self.selfnode.connections:
            if ( connection.node_a.nodeid == nodeid or
                 connection.node_b.nodeid == nodeid ):
                return connection.state
        return ConnectionState.UNKNOWN

    def find_node_by_address(self, address: str):
        assert isinstance(address, str)
        log.debug("in find_node_by_address on address %s", address)
        for node in self.nodes:
            log.debug("looping on node %s", node)
            if node.address == address:
                log.debug("found it")
                return node
        log.debug("did not find it")
        return None

    def setup_connections(self):
        self.nodes.sort(key=lambda x: str(x))

        assert len(self.nodes) > 1

        nconnections = 0
        for node_a in self.nodes:
            assert node_a is not None
            for node_b in self.nodes:
                assert node_b is not None
                if node_a is node_b:
                    continue
                if node_a > node_b:
                    log.debug("%s connects to %s", node_a, node_b)
                    nconnections += 1
                    # Note: We only need a ConnectionHandler object on the one
                    # running on "us".
                    if node_a is self.selfnode:
                        log.info("node_a is us, we are connecting")
                        conn_a = ConnectionHandler(
                            node_a,
                            node_b,
                            ConnectionState.INTENT_CONNECT,
                            self)
                        node_a.add_connection(conn_a)
                    elif node_b is self.selfnode:
                        log.info("node_b is us, we are listening")
                        conn_b = ConnectionHandler(
                            node_b,
                            node_a,
                            ConnectionState.INTENT_LISTEN,
                            self)
                        node_b.add_connection(conn_b)
                    else:
                        raise AssertionError("neither a or b is us")

        self.selfnode.start_networking()

    def send_msg(self, channel: ConnectionHandlerT, msg: ClusterMessage):
        channel.sending_queue.put(msg, block=True, timeout=TIMEOUT)

    def msg_for(self, nodeid: str, payload: str):
        """Send a message to nodeid nodeid. Throws a RuntimeError exception if
        this is not possible. If nodeid is None, we broadcast to all."""
        broadcast = False if nodeid is not None else True
        connections = []
        for conn in self.selfnode.connections:
            if conn.state == ConnectionState.CONNECTEDA or \
               conn.state == ConnectionState.CONNECTEDB:
                if broadcast:
                    connections.append(conn)
                else:
                    if conn.node_b.nodeid == nodeid:
                        connections.append(conn)
                        break
        if len(connections) == 0:
            msg = "Unable to address node %s - can't find an active connection to it" % nodeid
            raise ConnectionError(payload)

        for conn in connections:
            msg = ClusterMessage(fromid=conn.node_a.nodeid,
                                 toid=conn.node_b.nodeid,
                                 payload=payload)
            self.send_msg(conn, msg)

class ConnectionHandler(object):
    def __init__(self, node_a: ClusterNode, node_b: ClusterNode, initial_state: ConnectionState, manager: ConnectionManager, sock=None):
        """Takes the address and port of the connection, and the initial_state
        can be one of INTENT_CONNECT or INTENT_LISTEN. The last two states
        indicate whether we initiated the connection, or waited for it."""
        # node_a is always the initiating side of the connection
        self.node_a = node_a
        # node_b is always the listening side of the connection
        self.node_b = node_b
        self.state = initial_state
        self.manager = manager
        self.listen_limit = 5
        self.sending_queue = queue.Queue(maxsize=MAX_QUEUESIZE)
        self.receiving_queue = queue.Queue(maxsize=MAX_QUEUESIZE)
        # Functions to call when the connection successfully comes up
        self.success_callbacks = []
        # Functions to call when a successful connection drops, or an attempt
        # to initiate a connection fails
        self.error_callbacks = []
        self.partial = None
        if sock is not None:
            self.sock = sock
        else:
            self.sock = self.create_socket()

    def __str__(self):
        s = "ConnectionHandler: " + self.sig()
        return s

    def sig(self):
        """Simple identifier for logs."""
        if ( self.state == ConnectionState.INTENT_LISTEN or
             self.state == ConnectionState.LISTENING ):
            return f"{self.node_a.sig()}-LISTENING"
        else:
            return f"{self.node_a.sig()}-{self.node_b.sig()}"

    def add_success_callback(self, callback):
        """Add a callback for a successful connection. The only argument
        passed to the handler is this ConnectionHandler object."""
        self.success_callbacks.append(callback)

    def add_error_callback(self, callback):
        """Add a callback for an error. The only argument
        passed to the handler is this ConnectionHandler object."""
        self.error_callbacks.append(callback)

    def success(self):
        log.debug("calling all success callbacks: %d", len(self.success_callbacks))
        for callback in self.success_callbacks:
            try:
                callback(self)
            except Exception as err:
                log.error("success callback threw an exception: %s", err)

    def error(self):
        log.debug("calling all error callbacks: %d", len(self.error_callbacks))
        for callback in self.error_callbacks:
            try:
                callback(self)
            except Exception as err:
                log.error("error callback threw an exception: %s", err)

    def create_socket(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.setblocking(1)

        return sock

    def manage(self):
        """Kick off listening and connecting sockets. Return True if we still
        have to do something, False otherwise. Also use it to monitor existing
        connections."""
        assert self.sock is not None

        if self.state == ConnectionState.INTENT_CONNECT:
            try:
                log.info("%s Not yet connected to %s", BAD, self.node_b.sig())
                self.connect()
                self.success()
            except (ConnectionRefusedError, TimeoutError, InterruptedError) as err:
                log.error("%s", err)
                self.state = ConnectionState.INTENT_CONNECT
                # FIXME: update connection stats
                return True

        elif self.state == ConnectionState.INTENT_LISTEN:
            try:
                log.info("binding to %s:%d", self.node_a.address, self.node_a.listenport)
                self.sock.bind((self.node_a.address, self.node_a.listenport))
                self.listen()
            except Exception as err:
                log.error("%s", err)
                # FIXME: put into an error state?
                self.state = ConnectionState.INTENT_LISTEN
                return True

        elif self.state == ConnectionState.CONNECTEDA:
            log.info("%s connected to %s", GOOD, self.node_b.sig())

        elif self.state == ConnectionState.CONNECTEDB:
            log.info("%s connected from %s", GOOD, self.node_b.sig())

        else:
            log.debug("manage: state = %s", self.state)
        return False

    def drop_socket(self):
        try:
            log.warning("calling shutdown and close on socket")
            self.sock.shutdown(socket.SHUT_RDWR)
            self.sock.close()
        except OSError as err:
            log.error("%s", err)
        finally:
            log.debug("creating new socket")
            self.sock = self.create_socket()
            self.error()

    def valid_connection(self, addr):
        """Right now, return True if the IP is on our trusted list. But we need
        to enforce this with hard crypto."""
        address = addr[0]
        for connection in self.node_a.connections:
            if connection.node_b.address == address:
                return True
        return False

    def listen(self):
        if self.state == ConnectionState.INTENT_LISTEN:
            log.info("LISTENING for a connections")
            self.state = ConnectionState.LISTENING
            self.sock.listen(self.listen_limit)
            # Note: we block here - so call it from the thread
            log.debug("going into accept")
            new_sock, remote_addr = self.sock.accept()

            node_b = self.manager.find_node_by_address(remote_addr[0])
            if node_b is None:
                log.error("connection from unknown node: %s", remote_addr[0])
                return

            log.info("connection from %s", node_b.sig())

            if self.valid_connection(remote_addr):
                log.info("we trust this incoming connection")
                # We do not go to CONNECTEDB status, the new socket returned
                # from accept is in that state.
                conn = ConnectionHandler(self.node_a,
                                         node_b,
                                         ConnectionState.CONNECTEDB,
                                         self.manager,
                                         new_sock)
                self.node_a.add_connection(conn)
                # And call its success handler, as it's a new connection.
                conn.success()
            else:
                log.warning("we do not trust this endpoint - dropping")
                new_sock.shutdown(socket.SHUT_RDWR)
                new_sock.close()
        elif self.state == ConnectionState.LISTENING:
            log.debug("listen called but we are already listening")
        else:
            log.debug("listen called but state is %s", self.state)

    def connect(self):
        if self.state == ConnectionState.INTENT_CONNECT:
            log.info("CONNECTING to %s", self.sig())
            self.sock.connect((self.node_b.address, self.node_b.listenport))
            log.info("%s connected to %s", GOOD, self.sig())
            self.state = ConnectionState.CONNECTEDA
        else:
            log.error("%s connect called but state is %s", BAD, self.state)

    def reset_state(self):
        """Used on a disconnection error."""
        if self.state == ConnectionState.CONNECTEDA:
            self.state = ConnectionState.INTENT_CONNECT

        elif self.state == ConnectionState.CONNECTEDB:
            self.state = ConnectionState.INTENT_LISTEN

        else:
            log.warning("reset_state called on connection in state %s", self.state)

        log.debug("reset_state to %s", self.state)

    def send(self, msg: ClusterMessage):
        """Send message on socket."""
        try:
            msg.set_dtsent(datetime.datetime.now())
            j = msg.tojson()
            data = j.encode() + msgdelimb
            log.debug("sending msg '%s'", data.decode())
            log.debug("msg is %d bytes in size", len(data))

            nbytes = self.sock.send(data)
            log.debug("sent %d bytes", nbytes)
            if nbytes != len(data):
                raise ConnectionResetError("partial send")

        except (BrokenPipeError, ConnectionResetError) as err:
            log.error("failed to send on socket: %s", err)
            self.drop_socket()
            self.reset_state()

    def split_msgs(self, data: bytes) -> Tuple[List[str], bytes]:
        """Take the incoming data and split it into messages, and any 
        partial data remaining. We return a list of messages plus the
        partial as a tuple ([msgs], partial). The partial can be None."""
        partial = None
        msgs = []
        decodedbuf = data.decode()
        if decodedbuf.find(msgdelim) > -1:
            pieces = decodedbuf.split(msgdelim)
            if len(pieces) == 1:
                msgs.append(pieces[0])
            elif len(pieces) > 1:
                msgs = pieces[:-1]
                partial = pieces[-1].encode()
            else:
                raise AssertionError("invalid message parsing: '%s'" % decodedbuf)
        else:
            partial = data

        log.debug("returning msg count of %d", len(msgs))
        if partial is not None:
            log.debug("returning a partial of %d bytes", len(partial))
        else:
            log.debug("not returning a partial")
        return (msgs, partial)

    def receive(self) -> ClusterMessage:
        """Block and receive message on socket."""
        if ( self.state != ConnectionState.CONNECTEDB and 
             self.state != ConnectionState.CONNECTEDA ):
            log.warning("receive: socket not ready to receive: %s", self.state)
            return None

        try:
            log.debug("receive: state is %s - going into recv", self.state)
            data = self.sock.recv(BUFSIZE)
            if data is None:
                # Was it torn down?
                log.warning("receive: read Nothing from recv")
                return None
            log.debug("receive: received %d bytes from %s: %s",
                len(data), self.node_b.sig(), data)
            if len(data) > 0:
                if self.partial:
                    data = self.partial + data
                    self.partial = None
                msgs, partial = self.split_msgs(data)
                if partial is not None:
                    self.partial = partial
                for msg in msgs:
                    try:
                        cmsg = ClusterMessage(fromid=self.node_b.nodeid,
                                              toid=self.node_a.nodeid,
                                              payload=msg)
                        cmsg.fromjson()
                        cmsg.set_dtreceived(datetime.datetime.now())
                        log.info("received msg: %s", cmsg)
                        return cmsg
                    except queue.Full as err:
                        log.error("full receiving queue: %s", err)
                        continue
                    except Exception as err:
                        log.exception("possibly bad message format: %s", err)
            else:
                raise ConnectionResetError("0 bytes received")

        except (BrokenPipeError, ConnectionResetError) as err:
            log.error("receive: failed to receive on socket: %s", err)
            self.drop_socket()
            self.reset_state()
            return None

        except Exception as err:
            log.error("receive: unknown error: %s", err)
            self.drop_socket()
            self.reset_state()
            return None

def parse_args():
    global unicode
    parser = argparse.ArgumentParser()
    parser.add_argument("-d",
                        "--debug",
                        action="store_true",
                        default=False,
                        help="Debug logging")
    parser.add_argument("-s",
                        "--self-node",
                        action="store",
                        required=True,
                        help="The local node (nodeid:address:port)")
    parser.add_argument("-u",
                        "--no-unicode",
                        action="store_false",
                        dest="unicode",
                        default=True,
                        help="Disable using extra unicode symbols in logs")
    parser.add_argument('nodes',
                        nargs='+',
                        help="Other nodes in the cluster (nodeid:address:port)")
    args = parser.parse_args()
    if len(args.nodes) < 1:
        parser.print_help()
        sys.exit(1)

    if args.unicode:
        unicode = True
    else:
        unicode = False

    if args.debug:
        log.setLevel(logging.DEBUG)

    return args

def shutdown_handler(signum, frame):
    os.write(2, b"SHUTDOWN <===========\n")
    shutdown_asap.set()

def main():
    #setup_native_logger(logging.DEBUG)
    args = parse_args()
    signal.signal(signal.SIGINT, shutdown_handler)
    signal.signal(signal.SIGTERM, shutdown_handler)

    def success(info):
        log.info("success from %s", info)

    def error(info):
        log.error("error from %s", info)

    selfnode: ClusterNode = None
    try:
        nodeid, address, port = args.self_node.split(":")
        selfnode: ClusterNode = ClusterNode(nodeid, address, port)
        selfnode.add_success_callback(success)
        selfnode.add_error_callback(error)
    except ValueError:
        sys.stderr.write("Bad input: %s must be (nodeid, address:port)\n")
        sys.exit(1)

    manager = ConnectionManager(selfnode)
    manager.add_node(selfnode)

    for node in args.nodes:
        try:
            nodeid, address, port = node.split(":")
            cnode: ClusterNode = ClusterNode(nodeid, address, port)
            cnode.add_success_callback(success)
            cnode.add_error_callback(error)
            manager.add_node(cnode)

        except ValueError:
            sys.stderr.write("Bad input: %s must be (nodeid, address:port)\n", node)
            sys.exit(1)

    manager.setup_connections()

    while not shutdown_asap.is_set():
        log.info("thread count: %d", threading.active_count())
        log.info("selfnode is %s", selfnode.nodeid)
        if selfnode.nodeid == "vm1":
            try:
                log.info("testing send from vm1 to vm2")
                try:
                    manager.msg_for("vm2", "Hello")
                except ConnectionError as err:
                    log.error("%s", err)
            except RuntimeError as err:
                log.error("%s", err)
        log.debug("looking for waiting messages:")
        if manager.msgs_waiting():
            log.debug("there are messages waiting")
            for msg in manager.incoming_messages():
                log.info("===> Received: %s", msg)
                log.info("===> Flight time: %0.6f seconds", msg.tof())
        else:
            log.debug("there are no messages waiting")
        log.debug("main loop sleeping for %d seconds", LONG_SLEEPTIME)
        time.sleep(LONG_SLEEPTIME)
        log.debug("main loop waking up")

    log.debug("out of main loop, calling manager.shutdown()")
    manager.shutdown()

if __name__ == '__main__':
    main()
