import argparse
from datetime import (
    datetime, timezone, timedelta
)
import json
import os
import pathlib

from kafka import KafkaConsumer

from config import Config
from metrics_framework import (
    StreamArchiver, Consumer
)

class TopicConsumer(Consumer):
    """consumer the raw prices stream as a data source. """
    def __init__(self, topic:str, bootstrap_server:str):
        self.topic = topic
        self.bootstrap_server = bootstrap_server
        self.consumer =  KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_server
        )

    def poll(self):
        return self.consumer

class StoreMessages:
    """Stores a message in its own directory. 
    Stored with the storage timestamp and not the data timestamp."""
    
    def __init__(self, topic: str, data_dir:str = 'data'):
        self.topic = topic
        self.data_dir = data_dir
        self.topic_path = os.path.join(data_dir, topic)
        self.create_path(self)

    @staticmethod
    def create_path(self):
        pathlib.Path(self.topic_path).mkdir(parents=True, exist_ok=True)


    def log(self, message:str):
        ts = (datetime.now(timezone.utc) + timedelta(days=3)).timestamp() * 1e3
        with open(os.path.join(self.topic_path, f'{ts}.json'), 'w') as f:
            f.write(json.dumps(message))


class Archive(StreamArchiver):

    def __init__(self, topic: str):
        self.topic = topic
        self.config = Config()

    def consume(self):
        return TopicConsumer(
            topic=self.topic,
            bootstrap_server=self.config.bootstrap_server
        )

    def store(self) -> None:
        storage = StoreMessages(self.topic)
        while True:
            for message in self.consume().poll():
                json_message = json.loads(message.value.decode())
                storage.log(json_message)
                
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--topic', type=str)
    args = parser.parse_args()
    return args

def main():
    args = get_args()
    archiver = Archive(
        topic=args.topic
    )

    archiver.store()

if __name__ == '__main__':
    main()
