from abc import ABC, abstractmethod
from collections import deque
import random
import numpy as np
import time
import json


from kafka import KafkaProducer, KafkaConsumer

from config import Config
from metrics_framework import (
    StreamPipeline, Consumer, Processor, Producer 
)


SEED = 0
random.seed(SEED)
np.random.seed(SEED)

# used to slow down the streams for debugging
DELAY_FOR_DEV = Config().delay

class PricesConsumer(Consumer):
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

class PricesProducer(Producer):
    def __init__(self, 
        consumer:Consumer,
        processor:Processor,
        topic:str, 
        bootstrap_server:str,
        run_once:bool = False
    ):  
        """Producer for messages in the final 'prices' topic"""
        self.consumer = consumer
        self.processor = processor
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_server)
        self.run_once = run_once
        self.stream()
        
    def stream(self):
        continue_loop = True
        while continue_loop:
            for src_message in self.consumer.poll():
                message = self.processor.apply(src_message)
                self.producer.send(
                    self.topic, 
                    json.dumps(message).encode('utf-8')
                )
                
                if self.run_once: continue_loop = False

                if DELAY_FOR_DEV: time.sleep(1)

class ExpMovingAvgProcessor(Processor):
    """Calculates Moving Average and Exponential Moving Average"""
    def __init__(self, windows:int=100):
        self.isin_queues: dict = {}
        self.windows = windows
        self.alpha = 2/(windows+1)

    def init_isin_queue(self, isin:str):
        """Creates fixed length queue and last 
            exponential moving average per isin
        """
        if isin not in self.isin_queues:
            self.isin_queues[isin] = {
                'queue':deque(maxlen=self.windows),
                'prev_ema': None
            }

    def maintain_prev_ema(self, isin:str, price: float, ma: float, ema: float):

        """
        The exponential moving average can't be produced until n observations. 
        Even then it uses the simple moving average as the first, previous value t-1
        This method manages this state. 
        """
        
        if len(self.isin_queues[isin]['queue']) < self.windows:
            self.isin_queues[isin]['prev_ema'] = None
        elif not ema and len(self.isin_queues[isin]['queue']) == self.windows:
            self.isin_queues[isin]['prev_ema'] = ma
        elif len(self.isin_queues[isin]['queue']) == self.windows:
            self.isin_queues[isin]['prev_ema'] = ema

    def expo_moving_avg(
        self,
        isin: str,
        current_price: float,
        previous_ema: float
    ):
        """Calculates the ema if there are enough observations."""
        if self.isin_queues[isin]['prev_ema']:
            return self.alpha * current_price + (1-self.alpha) * previous_ema
        return

    @staticmethod
    def build_new_payload(message:dict, moving_avg:float, expo_moving_avg: float):
        """
        Sanitises the payload for publishing. Price is removes as this can be
        consumed from another topic. 
        """
        message['MOVING_AVG'] = moving_avg
        message['EXP_MOVING_AVG'] = expo_moving_avg
        del message['PRICE']

        return message

    def apply(self, message):
        """
        Calculates the moving average and exponential moving average and creates a new message. 
        """
        message = json.loads(message.value.decode())
        isin = message['ISIN']
        self.init_isin_queue(isin)

        # add price to te fixed size queue
        self.isin_queues[isin]['queue'].append(message['PRICE'])

        # calculate stats
        moving_avg = sum(self.isin_queues[isin]['queue'])/len(self.isin_queues[isin]['queue'])
        expo_moving_avg = self.expo_moving_avg(isin, message['PRICE'], self.isin_queues[isin]['prev_ema'])

        message_out = self.build_new_payload(message.copy(), moving_avg, expo_moving_avg)

        self.maintain_prev_ema(isin, message['PRICE'], moving_avg, expo_moving_avg)

        return message_out


class ExponentialMovingAverage(StreamPipeline):
    """Produces the measures of volatility. """
    def __init__(self):
        self.config = Config()

    def consume(self) -> PricesConsumer:
        return PricesConsumer(
            topic=self.config.prices_topic,
            bootstrap_server=self.config.bootstrap_server
        )
    
    def process(self) -> ExpMovingAvgProcessor:
        return ExpMovingAvgProcessor()

    def produce(self) -> PricesProducer:
        return PricesProducer(
            consumer=self.consume(),
            processor=self.process(), 
            topic=self.config.volatility_topic,
            bootstrap_server=self.config.bootstrap_server
        )

if __name__ == '__main__':
    ExponentialMovingAverage().produce()