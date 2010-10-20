#!/usr/bin/env python
# vim: ai ts=4 sts=4 et sw=4

import threading, logging
from Queue import Queue, Full, Empty
from ..lib.importlib import import_module

class BaseBackendThread(threading.Thread):
    def __init__(self, config):
        threading.Thread.__init__(self)
        self.config = config
        self.name = "%s main thread" % config['name']

        self.messages_outgoing = []
        self.messages_incoming = []
        self.requests_to_backend = Queue(maxsize=1)
        self.requests_from_backend = Queue(maxsize=1)
        self.commands = Queue(maxsize=1)

        self.kill_event = threading.Event()
        self.active_event = threading.Event()
        self.active_event.set()

        engine_module = import_module(config['broker']['engine'])

        self.broker = engine_module.BrokerThread(self.config, 
                                                 self.kill_event,
                                                 self.active_event,
                                                 self.messages_outgoing,
                                                 self.messages_incoming,
                                                 self.requests_to_backend,
                                                 self.requests_from_backend,
                                                 self.commands)
        self.broker.start()

    def process_requests(self):
        pass

    def process_commands(self):
        pass

    def wait(self, time=0.1):
        self.kill_event.wait(time)

    def kill(self):
        self.kill_event.set()

    @property
    def active(self):
        return self.active_event.is_set()

    @active.setter
    def active(self, value):
        if value:
            self.active_event.set()
        else:
            self.active_event.clear()

    def end(self):
        self.active = False
        logging.debug("%s ended" % self.name)
        self.broker.join()
