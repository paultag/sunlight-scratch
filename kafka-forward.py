#!/usr/bin/env python
import json
import threading, logging, time
from kafka.client import KafkaClient
from kafka.consumer import SimpleConsumer
from kafka.producer import SimpleProducer

import requests

# from opencivicdata.api.client import VagrantOCDAPI
# api = VagrantOCDAPI()
# XXX Fix the above.


def iternodes(message):
    report = message['report']
    for action, treport in report.items():
        for action, els in treport.items():
            for el in els:
                yield (action, el)


class OCDForwarder(object):
    # post-scrape-reports
    # incoming-data

    def run(self):
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client)
        self.consumer = SimpleConsumer(
            client,
            "post-scrape-forwardersss",
            "post-scrape-reports",
            max_buffer_size=None,
        )

        for message in self.consumer:
            message = json.loads(message.message.value)
            for action, id_ in iternodes(message):
                self.send_data(action, id_)

    def send_data(self, action, id_):
        url = "http://10.42.2.102/{}".format(id_)
        print(url)
        print(url)
        print(url)
        print(url)
        print(url)
        response = requests.get(url)
        data = response.json()  # XXX: Fixme
        if response.status_code != 200:
            raise ValueError("Bailed: {}".format(data.text))
        print("Working on {}".format(id_))
        self.producer.send_messages('incoming-data', json.dumps({
            "action": action,
            "data": data,
        }))


if __name__ == "__main__":
    logging.basicConfig(
        format='%(asctime)s.%(msecs)s:%(name)s:%(thread)d:%(levelname)s:%(process)d:%(message)s',
        level=logging.DEBUG
        )
    OCDForwarder().run()
