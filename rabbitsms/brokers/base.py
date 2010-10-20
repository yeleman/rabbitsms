#!/usr/bin/env python
# vim: ai ts=4 sts=4 et sw=4

import threading, logging

class BaseBrokerThread(threading.Thread):
    def __init__(self, config, kill_event, active_event,
                 messages_outgoing,
                 messages_incoming,
                 requests_to_backend,
                 requests_from_backend,
                 commands):
        threading.Thread.__init__(self)

        self.config = config
        self.name = "%s broker thread" % self.config['name']
        self.kill_event = kill_event
        self.active_event = active_event

        self.messages_outgoing = messages_outgoing
        self.messages_incoming = messages_incoming
        self.requests_to_backend = requests_to_backend
        self.requests_from_backend = requests_from_backend
        self.commands = commands

    def end(self):
        logging.debug("%s ended" % self.name)
