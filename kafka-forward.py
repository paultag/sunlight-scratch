#!/usr/bin/env python
import os
import uuid
import json
import threading, logging, time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

import requests

from opencivicdata.api.client import VagrantOCDAPI
api = VagrantOCDAPI()

DATA_ROOT = os.path.abspath("./data")


def iternodes(message):
    report = message['report']
    for action, treport in report.items():
        for action, els in treport.items():
            for el in els:
                yield (action, el)


class OCDForwarder(object):
    # post-scrape-reports
    # incoming-data

    def run(self, quick=None):
        self.quick = False if quick is None else True

        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client)
        self.consumer = SimpleConsumer(
            client,
            str(uuid.uuid4()).encode(),
            b"post-scrape-reports",
            max_buffer_size=None,
        )

        for message in self.consumer:
            message = json.loads(message.message.value.decode('utf-8'))
            for _, id_ in iternodes(message):
                self.fetch_data(id_)

            for action, id_ in iternodes(message):
                self.send_data(action, id_)

    def send_data(self, action, id_):
        klass, _ = id_.split("/", 1)
        if klass in ['ocd-membership', 'ocd-post']:
            return

        path = os.path.join(DATA_ROOT, id_)
        with open(path, 'r') as fd:
            data = json.load(fd)

        self.producer.send_messages('incoming-data', json.dumps({
            "action": action,
            "data": data,
        }).encode())


    def fetch_data(self, id_, fail=0):
        klass, _ = id_.split("/", 1)
        if klass in ['ocd-membership', 'ocd-post']:
            """
            TODO: What to do about the above
            """
            return

        path = os.path.join(DATA_ROOT, id_)
        root = os.path.dirname(path)

        if self.quick and os.path.exists(path):
            return

        try:
            data = api._get_object(id_)
        except ValueError:
            if fail > 3:
                raise
            return self.fetch_data(id_, fail=(fail + 1))

        if not os.path.exists(root):
            os.makedirs(root)

        with open(path, 'w') as fd:
            json.dump(data, fd)


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    OCDForwarder().run(*sys.argv[1:])
