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
    formatter = logging.Formatter('%(asctime)s: [%(levelname)s] <%(threadName)s> %(name)s:%(lineno)s %(message)s', datefmt="%m-%d-%YT%I:%M:%S%z")
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

class ConnectionError(RuntimeError):
    pass

class ConnectionState(Enum):
    """INTENT_CONNECT - we intend to initiate the connection
       INTENT_LISTEN - we intend to listen for a connection
       ACCEPT - we are bound and blocked on accept
       CONNECTEDA - we are connected, initiated from node_a
       CONNECTEDB - we are connected, initiated from node_b
       UNKNOWN - either don't know the state, or don't know the node"""
    INTENT_CONNECT = 1
    INTENT_LISTEN = 2
    ACCEPT = 3
    CONNECTEDA = 4
    CONNECTEDB = 5
    ERRORA = 6
    ERRORB = 7
    UNKNOWN = 8

class ClusterNode(object):
    def __init__(self, nodeid, address, listenport):
        assert nodeid is not None
        assert address is not None
        assert listenport is not None

        self.address = address
        self.listenport = int(listenport)
        self.nodeid = nodeid
        self.connections: List[ConnectionHandlerT] = []
        self.listen_thread = None
        self.success_callbacks = []
        self.error_callbacks = []
        self.rlock = threading.RLock()
        log.debug("ClusterNode.ctor()")

    def __del__(self):
        log.debug("ClusterNode.dtor()")

    def __gt__(self, other):
        if str(self) > str(other):
            return True
        else:
            return False

    def __str__(self):
        return "ClusterNode: %s" % (self.nodeid)

    def remove_connection(self, conn: ConnectionHandlerT) -> bool:
        """Look for and remove conn from our list of connections. Return
        True if found, False otherwise."""
        new_list = []
        found = False
        with self.rlock:
            for connection in self.connections:
                if connection is conn:
                    found = True
                    continue
                new_list.append(connection)
            self.connections = new_list
        return found

    def add_success_callback(self, callback):
        """Add a success callback to all connections."""
        with self.rlock:
            self.success_callbacks.append(callback)
            for conn in self.connections:
                conn.add_success_callback(callback)

    def add_error_callback(self, callback):
        """Add an error callback to all connections."""
        with self.rlock:
            self.error_callbacks.append(callback)
            for conn in self.connections:
                conn.add_error_callback(callback)

    def sig(self):
        return f"{self.nodeid}-{self.address}:{self.listenport}"

    def add_connection(self, conn: ConnectionHandlerT):
        log.debug("adding connection %s to %s", conn, self)
        with self.rlock:
            self.connections.append(conn)
            for callback in self.success_callbacks:
                conn.add_success_callback(callback)
            for callback in self.error_callbacks:
                conn.add_error_callback(callback)

            if conn.state == ConnectionState.INTENT_CONNECT:
                connthread = ConnectionThread(conn, name=conn.sig())
                conn.threads.append(connthread)
                log.debug("added an INTENT_CONNECT thread")

            elif conn.state == ConnectionState.INTENT_LISTEN:
                connthread = ConnectionThread(conn, name=conn.sig())
                if self.listen_thread is None:
                    log.debug("discarding redundant listening thread")
                else:
                    conn.threads.append(connthread)
                    log.debug("added an INTENT_LISTEN thread")

            elif conn.state == ConnectionState.CONNECTEDB:
                connthread = ConnectionThread(conn, name=conn.sig())
                conn.threads.append(connthread)
                log.debug("added a CONNECTEDB thread")
                # This type of thread is added after we're up and running, so start
                # it now.
                connthread.start()
                # and call any success callbacks on it, since this is a new
                # connection that has successfully come up
                conn.success()

            else:
                raise AssertionError("wrong state for add_connection: %s", conn.state)

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

class ClusterManager(object):
    """Manage the connections between nodes and routing of messages."""
    def __init__(self, selfnode: ClusterNode):
        assert selfnode is not None
        self.selfnode: ClusterNode = selfnode
        self.nodes: List[ClusterNode] = []
        log.debug("ConnectionManager.ctor(), set selfnode to %s", self.selfnode)

    def __del__(self):
        log.debug("ConnectionManager.dtor()")

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
        for conn in self.selfnode.connections:
            log.info("conn %s", conn)
            for thread in conn.threads:
                if thread.is_alive():
                    log.info("joining %s", thread)
                    thread.shutdown()
                    thread.join()
                else:
                    log.debug("thread is not alive")

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
        with open("cluster.dot", "w") as dotfile:
            dotfile.write("digraph Nodes {\n")
            for i, node_a in enumerate(self.nodes):
                assert node_a is not None
                for node_b in self.nodes:
                    assert node_b is not None
                    if i == 0:
                        dotfile.write("\"%s\" [shape=rectangle,label=\"%s %s %d\"]\n" % (node_b, node_b.nodeid, node_b.address, node_b.listenport))
                    if node_a is node_b:
                        continue
                    if node_a > node_b:
                        log.debug("%s connects to %s", node_a, node_b)
                        dotfile.write("\"%s\" -> \"%s\"\n" % (node_a, node_b))
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
                            conn_a.init_thread()
                        elif node_b is self.selfnode:
                            log.info("node_b is us, we are listening")
                            conn_b = ConnectionHandler(
                                node_b,
                                node_a,
                                ConnectionState.INTENT_LISTEN,
                                self)
                            node_b.add_connection(conn_b)
                            conn_b.init_thread()
                        else:
                            log.warning("neither a or b is us - not involved")
            dotfile.write("}\n")

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

    def monitor_connections(self):
        """Run regularly to catch errors and reset the state on connection handlers to allow for re-attempts."""
        log.info("======================================================")
        log.info("======= status report ================================")
        log.info("======================================================")
        log.info("nthreads: %d", threading.active_count())
        for thread in threading.enumerate():
            log.info("    thread = %s", thread.name)
        for conn in self.selfnode.connections:
            log.debug("checking on selfnode connection %s", conn)
            if conn.is_connected():
                log.info("FIXME: print a status report for %s", conn)
            elif conn.in_error():
                log.error("Found a handler in the ERROR state: %s", conn)
                # If it's an A-side, we need to reestablish, otherwise just get rid of the handler.
                if conn.state == ConnectionState.ERRORA:
                    log.debug("ERRORA - moving to INTENT_CONNECT")
                    conn.state = ConnectionState.INTENT_CONNECT
                elif conn.state == ConnectionState.ERRORB:
                    log.debug("ERRORB - can drop this handler, expect a new one incoming")
                    self.selfnode.remove_connection(conn)
                    continue

class ConnectionHandler(object):
    def __init__(self, node_a: ClusterNode, node_b: ClusterNode, initial_state: ConnectionState, manager: ClusterManager, sock=None):
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
        self.threads = []
        log.debug("ConnectionHandler.ctor()")
        self.rlock = threading.RLock()

    def __del__(self):
        log.debug("ConnectionHandler.dtor()")
        for thread in self.threads:
            if thread.is_alive():
                log.debug("joining thread: %s", thread)
                thread.join()

    def __str__(self):
        s = "ConnectionHandler: " + self.sig()
        return s

    def init_thread(self):
        with self.rlock:
            # FIXME: should we guard against threads already in place?
            connthread = ConnectionThread(self, name=self.sig())
            if self.state == ConnectionState.INTENT_CONNECT:
                self.threads.append(connthread)
                log.debug("added an INTENT_CONNECT thread")

            elif self.state == ConnectionState.INTENT_LISTEN:
                if self.node_b.listen_thread is not None:
                    self.node_b.listen_thread = connthread
                    self.threads.append(connthread)
                else:
                    log.debug("discarding extra listen thread")

            elif self.state == ConnectionState.CONNECTEDB:
                self.listen_threads.append(connthread)
                log.debug("added a CONNECTEDB thread")
                # and call any success callbacks on it, since this is a new
                # connection that has successfully come up
                self.success()

            else:
                raise AssertionError("wrong state for add_connection: %s", conn.state)

            connthread.start()

    def sig(self):
        """Simple identifier for logs."""
        if ( self.state == ConnectionState.INTENT_LISTEN or
             self.state == ConnectionState.ACCEPT ):
            return f"{self.node_a.sig()}-ACCEPT"
        else:
            return f"{self.node_a.sig()}-{self.node_b.sig()}"

    def error_state(self):
        """Examine the current state and go into the appropriate error state."""
        if self.state == ConnectionState.CONNECTEDA:
            self.state = ConnectionState.ERRORA
        elif self.state == ConnectionState.INTENT_CONNECT:
            self.state = ConnectionState.ERRORA
        elif self.state == ConnectionState.CONNECTEDB:
            self.state = ConnectionState.ERRORB
        elif self.state == ConnectionState.INTENT_LISTEN:
            pass
        elif self.state == ConnectionState.ACCEPT:
            self.state = ConnectionState.INTENT_LISTEN
        else:
            log.error("error_state: not sure how to handle %s", self.state)

    def in_error(self) -> bool:
        """Return a boolean indicating whether we are in an error state."""
        if self.state == ConnectionState.ERRORA or self.state == ConnectionState.ERRORB:
            return True
        else:
            return False

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
            while True:
                try:
                    log.info("LISTENING for a connections")
                    self.sock.listen(self.listen_limit)
                    # Note: we block here - so call it from the thread
                    self.state = ConnectionState.ACCEPT
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

                except Exception as err:
                    log.error("fatal error in accept loop: %s", err)
                finally:
                    # FIXME: should we go into ERROR state and let it be handled higher up?
                    self.state = ConnectionState.INTENT_LISTEN
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

        else:
            log.warning("reset_state called on connection in state %s", self.state)

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
            raise ConnectionResetError(err)

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

    def is_connected(self):
        if ( self.state != ConnectionState.CONNECTEDB and 
             self.state != ConnectionState.CONNECTEDA ):
            return False
        else:
            return True

    def receive(self) -> ClusterMessage:
        """Block and receive message on socket."""
        if not self.is_connected():
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
            raise ConnectionResetError(err)

        except Exception as err:
            log.error("receive: unknown error: %s", err)
            self.drop_socket()
            self.reset_state()
            raise ConnectionResetError(err)

class ConnectionWritingThread(threading.Thread):
    def __init__(self, conn: ConnectionHandler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("ConnectionWritingThread.ctor()")
        self.conn = conn
        # Add to list of threads on the connection handler to allow for thread
        # shutdown on a connection problem.
        self.conn.threads.append(self)

    def __del__(self):
        log.debug("ConnectionWritingThread.dtor()")

    def run(self):
        log.debug("ConnectionWritingThread starting")
        while not shutdown_asap.is_set():
            if not self.conn.is_connected():
                time.sleep(SHORT_SLEEPTIME)
                continue
            try:
                msg = self.conn.sending_queue.get(block=True, timeout=TIMEOUT)
                log.debug("found message on sending queue: %s", msg)
                self.conn.send(msg)
            except queue.Empty:
                continue
            except ConnectionResetError:
                log.error("connection reset: putting %s into error state", self.conn)
                self.conn.error_state()
                # Exit the thread
                return

    def shutdown(self):
        pass

class ConnectionReadingThread(threading.Thread):
    def __init__(self, conn: ConnectionHandler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("ConnectionReadingThread.ctor()")
        self.conn = conn
        # Add to list of threads on the connection handler to allow for thread
        # shutdown on a connection problem.
        self.conn.threads.append(self)
        self.partial = None

    def __del__(self):
        log.debug("ConnectionReadingThread.dtor()")

    def run(self):
        log.debug("ConnectionReadingThread starting")
        while not shutdown_asap.is_set():
            try:
                msg = self.conn.receive()
                if msg is not None:
                    log.debug("reading thread: received a message: %s", msg)
                    self.conn.receiving_queue.put(msg, block=True, timeout=TIMEOUT)
                else:
                    log.debug("reading thread: receive returned None")
                    time.sleep(SHORT_SLEEPTIME)
            except ConnectionResetError:
                log.error("connection reset: putting %s into error state", self.conn)
                self.conn.error_state()
                # Exit the thread
                return

    def shutdown(self):
        pass

class ConnectionThread(threading.Thread):
    def __init__(self, conn: ConnectionHandler, *args, **kwargs):
        super().__init__(*args, **kwargs)
        log.debug("ConnectionThread.ctor()")
        self.conn = conn
        self.sleeptime_connected = LONG_SLEEPTIME
        self.sleeptime_not_connected = SHORT_SLEEPTIME
        self.read_thread = None
        self.write_thread = None

    def __del__(self):
        log.debug("ConnectionThread.dtor()")
        self.shutdown()

    def shutdown(self):
        if self.read_thread is not None:
            if self.read_thread.is_alive():
                log.info("joining read thread")
                self.read_thread.shutdown()
                self.read_thread.join()
                self.read_thread = None
            else:
                log.debug("read thread is not alive")
        if self.write_thread is not None:
            if self.write_thread.is_alive():
                log.info("joining write thread")
                self.write_thread.shutdown()
                self.write_thread.join()
                self.write_thread = None
            else:
                log.debug("write thread is not alive")


    def run(self):
        log.debug("ConnectionThread starting")
        while not shutdown_asap.is_set():
            try:
                # Go into listen state, or attempt to connect, or status report
                self.conn.manage()
                if self.conn.sock is None:
                    log.debug("waiting for socket")
                    time.sleep(SHORT_SLEEPTIME)
                    continue

                # Only fully connected handlers need their own threads.
                if self.conn.is_connected():
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

                # Error checks
                elif self.conn.in_error():
                    # FIXME: yikes
                    log.debug("found a connection in ERROR state")
                    self.conn.node_a.remove_connection(self.conn)
                    self.conn.node_b.remove_connection(self.conn)
                    self.conn = None
                    # Tell the tracker we're exiting.
                    return
                time.sleep(LONG_SLEEPTIME)

            except Exception as err:
                log.error("ConnectionThread fatal error caught: %s", err)

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
    signal.alarm(1)

def main():
    setup_native_logger(logging.DEBUG)
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
        # We only need callbacks on selfnode, it's where all the action is.
        selfnode.add_success_callback(success)
        selfnode.add_error_callback(error)
    except ValueError:
        sys.stderr.write("Bad input: %s must be (nodeid, address:port)\n")
        sys.exit(1)

    manager = ClusterManager(selfnode)
    manager.add_node(selfnode)

    for node in args.nodes:
        try:
            nodeid, address, port = node.split(":")
            cnode: ClusterNode = ClusterNode(nodeid, address, port)
            manager.add_node(cnode)

        except ValueError:
            sys.stderr.write("Bad input: %s must be (nodeid, address:port)\n", node)
            sys.exit(1)

    manager.setup_connections()

    while not shutdown_asap.is_set():
        log.info("thread count: %d", threading.active_count())
        log.info("selfnode is %s", selfnode.nodeid)
        # And call monitor regularly to start connections and restart them if needed.
        manager.monitor_connections()
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
