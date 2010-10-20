#!/usr/bin/env python
# vim: ai ts=4 sts=4 et sw=4

from __future__ import absolute_import

import logging
from Queue import Queue, Full, Empty
from socket import error as socket_error
from socket import timeout as socket_timeout

from amqplib.client_0_8 import AMQPConnectionException
from kombu.connection import BrokerConnection
from kombu import Exchange, Consumer, Producer
from kombu import Queue as KomboQueue

from .base import BaseBrokerThread

CONNECTION_EXCEPTIONS = (AMQPConnectionException, socket_error, IOError)

class BrokerThread(BaseBrokerThread):
    def __init__(self, *args, **kwargs):
        super(BrokerThread, self).__init__(*args, **kwargs)

        self.queued_outgoing_message = None

        self.incoming_producer = None
        self.outgoing_consumer = None
        self.outgoing_consumer_active = False
        self.connected = False

    def create_broker_connection(self):
        broker = self.config['broker']
        return BrokerConnection(hostname=broker['host'],
                                port=broker['port'],
                                userid=broker['user'],
                                password=broker['password'],
                                virtual_host=broker['vhost'],
                                transport=broker.get('transport', 'amqplib'))

    def connect(self):
        logging.debug("%s trying to connect to broker" % self.name)
        self.conn = self.create_broker_connection()
        try:
            self.conn.connect()
        except (CONNECTION_EXCEPTIONS, socket_timeout):
            return False
        else:
            logging.debug("%s successfully connected to broker" % self.name)
            self.channel = self.conn.channel()
            return True

    def attach_queues(self):
        message_exchange = Exchange(name='messages', type='topic')
        requests_exchange = Exchange(name='requests', type='topic')
        command_exchange = Exchange(name='commands', type='topic')

        key = '%s.outgoing' % self.config['name']
        outgoing_queue = KomboQueue(exchange=message_exchange, name=key,
                                    routing_key=key)

        key = '%s.command' % self.config['name']
        command_queue = KomboQueue(exchange=command_exchange, name=key,
                                   routing_key=key)

        key = '%s.request' % self.config['name']
        request_queue = KomboQueue(exchange=requests_exchange, name=key,
                                    routing_key=key)
        
        self.outgoing_consumer = Consumer(self.channel, outgoing_queue)
        self.outgoing_consumer.register_callback(self.consumer_callback)

        other_consumers = Consumer(self.channel,
                                   [command_queue, request_queue])
        other_consumers.register_callback(self.consumer_callback)
        other_consumers.consume()

        self.incoming_producer = Producer(self.channel,
                                          exchange=message_exchange,
                                          serializer='json')

    def consumer_callback(self, message_data, message):
        if 'type' not in message_data or \
           message_data['type'] not in ['message', 'command', 'request']:
            message.reject()
            return

        if message_data['type'] == 'message':
            if self.messages_outgoing or not self.active_event.is_set():
                message.requeue()
            else:
                if self.queued_outgoing_message:
                    self.queued_outgoing_message.ack()
                self.queued_outgoing_message = message
                self.messages_outgoing.append(message_data)

    def requeue_queued_outgoing_message(self):
        try:
            self.messages_outgoing.pop()
        except IndexError:
            pass
        else:
            if self.connected and self.queued_outgoing_message:
                self.queued_outgoing_message.requeue()

    def run(self):
        incoming_key = '%s.incoming' % self.config['name']
        while not self.kill_event.is_set():
            if not self.connected:
                if self.connect():
                    self.connected = True
                    self.attach_queues()
                else:
                    self.kill_event.wait(5)
                    continue

            if self.active_event.is_set() and \
               not self.outgoing_consumer_active:
                self.outgoing_consumer.consume()
                self.outgoing_consumer_active = True
                logging.debug("%s activating outgoing message consumer" \
                             % self.name)
            elif not self.active_event.is_set() and \
                 self.outgoing_consumer_active:
                logging.debug("%s canceling outgoing message consumer" \
                             % self.name)
                self.requeue_queued_outgoing_message()
                self.outgoing_consumer.cancel()
                self.outgoing_consumer_active = False

            if self.queued_outgoing_message and \
               not self.messages_outgoing:
                self.queued_outgoing_message.ack()
                self.queued_outgoing_message = None

            if self.active_event.is_set() and self.messages_incoming:
                try:
                    self.incoming_producer \
                        .publish(self.messages_incoming[0],
                                 routing_key=incoming_key)
                except (CONNECTION_EXCEPTIONS, socket_timeout):
                    self.connected = False
                    continue
                except IndexError:
                    pass
                else:
                    msg = self.messages_incoming.pop(0)
                    logging.info("%s sending message to broker: %s " \
                                 % (self.name, msg))

            try:
                logging.log(1, "%s starting to drain_events" % self.name)
                self.conn.drain_events(timeout=.1)
            except socket_timeout:
                pass
            except CONNECTION_EXCEPTIONS:
                self.connected = False
            self.kill_event.wait(.1)
            logging.log(1, "%s bottom of main run loop" % self.name)

        if self.connected:
            self.requeue_queued_outgoing_message()
            self.conn.close()

        self.end()
