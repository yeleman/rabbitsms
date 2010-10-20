#!/usr/bin/env python
# vim: ai ts=4 sts=4 et sw=4

from __future__ import absolute_import

import logging

import gammu

from ..lib.rfc3339 import rfc3339
from .base import BaseBackendThread

CONNECTION_EXCEPTIONS = (gammu.ERR_NOTCONNECTED,
                         gammu.ERR_DEVICEWRITEERROR,
                         gammu.ERR_TIMEOUT,
                         gammu.ERR_UNKNOWN)

EMPTY_EXCEPTIONS = (gammu.ERR_INVALIDLOCATION,
                    gammu.ERR_EMPTY,
                    gammu.ERR_NOTSUPPORTED)

class Report(object):
    pass

class SimpleMessage(object):
    def __init__(self, sm, msg):
        self.sm = sm
        self.folder = msg['Folder']
        self.location = msg['Location']
        self.text = msg['Text']
        self.identity = msg['Number']
        self.received_timestamp = msg['DateTime']
        self.smsc_timestamp = msg['SMSCDateTime']
        self.num_parts = 1
        self.sent = False

    def get_dict(self):
        return  {'type':'message',
                 'text': self.text,
                 'parts': self.num_parts,
                 'identity': self.identity,
                 'received_timestamp': rfc3339(self.received_timestamp),
                 'smsc_timestamp': rfc3339(self.smsc_timestamp)}

    def delete(self):
        try:
            self.sm.DeleteSMS(self.folder, self.location)
        except EMPTY_EXCEPTIONS:
            pass

class MessagePart(SimpleMessage):
    def __init__(self, sm, msg):
        super(MessagePart, self).__init__(sm, msg)
        self.part_num = msg['UDH']['PartNumber']

class MultipartMessage(object):
    def __init__(self, sm, first_part):
        self.sm = sm
        self.num_parts = first_part['UDH']['AllParts']
        self.parts = []
        self.add_part(first_part)
        self.sent = False

    def process_part(self, part):
        part_num = part['UDH']['PartNumber']
        if len(filter(lambda x: x.part_num == part_num, self.parts)) == 0:
            self.add_part(part)

    @property
    def text(self):
        return u''.join([x.text for x in \
                            sorted(self.parts, key=lambda x: x.part_num)])

    def get_dict(self):
        if not self.parts:
            return
        ret_dict = self.parts[-1].get_dict()
        ret_dict.update({'text': self.text, 'parts': self.num_parts})
        return ret_dict

    def delete(self):
        for part in self.parts:
            part.delete()

    def add_part(self, msg):
        self.parts.append(MessagePart(self.sm, msg))

    def is_complete(self):
        return len(self.parts) == self.num_parts

class IncomingMsgProcessor(object):
    def __init__(self, sm):
        self.sm = sm
        self.cache = {}

    def process_simple_msg(self, msg):
        msg_hash = hash(''.join([msg['Number'],
                                 str(msg['DateTime']), 
                                 msg['Text']]))
        if not msg_hash in self.cache:
            self.cache[msg_hash] = SimpleMessage(self.sm, msg)

    def process_part_msg(self, msg):
        if msg['UDH']['ID8bit'] != -1:
            ref = msg['UDH']['ID8bit']
        elif msg['UDH']['ID16bit'] != -1:
            ref = msg['UDH']['ID16bit']
        else:
            return
        msg_hash = hash(''.join([str(ref), msg['Number']]))
        if not msg_hash in self.cache:
            self.cache[msg_hash] = MultipartMessage(self.sm, msg)
        else:
            self.cache[msg_hash].process_part(msg)

    def mark_as_sent(self, msg_hash):
        if msg_hash in self.cache:
            self.cache[msg_hash].sent = True

    def complete_messages(self):
        msgs = {}
        for msg_hash, msg in self.cache.items():
            if msg.num_parts == 1 or msg.num_parts == len(msg.parts):
                msgs[msg_hash] = msg
        return msgs

    def delete(self, msg_hash):
        msg = self.cache[msg_hash]
        print 'deleting from modem: ', self.cache[msg_hash]
        msg.delete()
        del(self.cache[msg_hash])

    def get_message(self):
        complete_messages = self.complete_messages()
        if not complete_messages:
            return None, None
        msg_hash = self.cache.keys()[0]
        return msg_hash, complete_messages[msg_hash].get_dict()

    def process(self, msg_list):
        for msg in msg_list:
            if not 'Type' in msg:
                return
            if msg['Type'] == 'Status_Report':
                pass
            elif msg['Type'] == 'Deliver':
                if msg['UDH']['Type'] == 'NoUDH':
                    self.process_simple_msg(msg)
                elif msg['UDH']['Type'] == 'ConcatenatedMessages':
                    self.process_part_msg(msg)

class BackendThread(BaseBackendThread):
    def __init__(self, config):
        super(BackendThread, self).__init__(config)
        self.gammu_config = {
                'Connection': config.get('connection', 'at'),
                'Device': config.get('device', '/dev/ttyUSB0')}
        self.connected = False
        self.queued_incoming_msg_hash = None

    def connect(self):
        try:
            self.sm.Init()
        except gammu.ERR_DEVICENOTEXIST:
            logging.warning("%s does not exist" % self.gammu_config['Device'])
        except gammu.ERR_DEVICEOPENERROR, gammu.ERR_UNKNOWN:
            logging.warning("Unkown problem opening %s" % \
                            self.gammu_config['Device'])
        except gammu.ERR_NOSIM:
            logging.warning("No SIM card in modem at %s" % \
                            self.gammu_config['Device'])
        except gammu.ERR_TIMEOUT:
            logging.warning("Timeout waiting for %s" % \
                            self.gammu_config['Device'])
        else:
            self.connected = True
            return True
        return False

    def get_all_messages(self):
        messages = []
        try:
            msg = self.sm.GetNextSMS(0, True)[0]
        except EMPTY_EXCEPTIONS:
            pass
        else:
            messages.append(msg)
            while True:
                try:
                    msg = self.sm.GetNextSMS(0, False, msg['Location'])[0]
                except EMPTY_EXCEPTIONS:
                    break
                else:
                    messages.append(msg)
        return messages

    def run(self):
        self.sm = gammu.StateMachine()
        self.sm.SetConfig(0,self.gammu_config)
        self.processor = IncomingMsgProcessor(self.sm)

        while not self.kill_event.is_set():
            if not self.connected and not self.connect():
                self.wait(3)
                continue

            self.process_commands()
            self.process_requests()

            self.queue_for_broker()

            if not self.active:
                try:
                    self.messages_incoming.pop(0)
                except IndexError:
                    pass
                self.queued_incoming_msg_hash = None
                self.wait()
                continue

            try:
                self.processor.process(self.get_all_messages())
            except CONNECTION_EXCEPTIONS:
                self.connected = False
                continue

            for msg in self.processor.cache.values():
                print msg.get_dict()

            self.wait(1)
        self.end()

    def queue_for_broker(self):
        if not self.messages_incoming:
            if self.queued_incoming_msg_hash:
                self.processor.delete(self.queued_incoming_msg_hash)
                self.queued_incoming_msg_hash = None
            msg_hash, msg = self.processor.get_message()
            if msg:
                self.queued_incoming_msg_hash = msg_hash
                self.messages_incoming.append(msg)

    def get_ussd(self):
        pass

    def get_signal_quality(self):
        pass
