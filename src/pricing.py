import random
import hashlib
import numpy as np
import datetime
import pytz
import time
import json


from kafka import KafkaProducer

from config import Config
from metrics_framework import (
    StreamPipeline, Consumer, Processor, Producer 
)

SEED = 0
random.seed(SEED)
np.random.seed(SEED)

# used to slow down the streams for debugging
DELAY_FOR_DEV = Config().delay


class RawPricesConsumer(Consumer):
    """
    Consumer for what I've labelled the 'raw prices'. This is in place of a Consumer 
    from a Kafka stream, and api or some ther system. 
    """
    def __init__(self, 
        suffixes: list = ['GB','IT','ES', 'DE', 'US', 'ES'],
        n_isins: int = 3
    ):
        self.seed = SEED
        self.suffixes = suffixes
        self.len_suffixes = len(suffixes)
        self.current_price = {}
        self.n_isins = n_isins
        self.iw = {k: (r+2)*4/100 for r, k in enumerate(self.suffixes)}
        self.utc_timezone = pytz.timezone('UTC')
        self.static_dt = datetime.datetime(year=2022,month=10, day=15, tzinfo=self.utc_timezone)
        self.static_dt_unix = self.static_dt.timestamp()
    
    def _get_random_isin(self):
        
        random_isin_int = random.randint(0, self.n_isins-1)
        hash = hashlib.md5(f'{random_isin_int}'.encode()).hexdigest()
        
        isin_suffix = hash[:10].upper()
        
        isin_prefix_index = int(hash, 16)%self.len_suffixes
        isin_prefix = self.suffixes[isin_prefix_index]

        isin = isin_prefix + isin_suffix
        decimal = random.randint(0, 100)/100
        adjustor = random.randint(0,5)
        self.current_price.setdefault(isin, adjustor+decimal)
        return isin

    def _get_isin_next_price(self, isin):
        last_price = self.current_price[isin]
        w = self.iw[isin[:2]]
        # -1 and 1
        r = np.random.uniform(low=-1, high=1)
        per_change = 2*w*r/100
  
        last_price = (1+per_change)*last_price 
        self.current_price[isin] = last_price
        return float("{:.6f}".format(last_price))

    def set_offset_at_datetime(self, dt_utc: datetime.datetime):
        """sets consequential .poll() calls starting from dt_utc

        Args:
            dt_utc (datetime.datetime): time to set the queue to in UTC
        """        
        assert isinstance(dt_utc, datetime.datetime)
        dt_utc = dt_utc.replace(tzinfo=self.utc_timezone)

        dt_now = datetime.datetime.utcnow().replace(tzinfo=self.utc_timezone)
        assert dt_utc<=dt_now, f'dt {dt_utc} must be less than now {dt_now}'
        assert dt_utc>=self.static_dt, f'dt {dt_utc} must be greater than {self.static_dt}'
        self.static_dt_unix = dt_utc.timestamp()
    
    def poll(self):
        """retrieves a price for a security

        Returns:
            dict: with ISIN, PRICE and DATETIME in UTC
        """        
        isin = self._get_random_isin()
        price = self._get_isin_next_price(isin)
        # 1- 10 seconds per new update price
        self.static_dt_unix = self.static_dt_unix + random.randint(1,6)
        dt = datetime.datetime.utcfromtimestamp(self.static_dt_unix).strftime('%Y-%m-%d %H:%M:%S.0')
        t_now = time.time()
        if self.static_dt_unix >= t_now:
            time.sleep(self.static_dt_unix-t_now)

        return {'ISIN': isin, 'PRICE': price, 'DATETIME': dt}

class RawPricesProcessor(Processor):
    """Performs no transformation of the data, just passes it through. """
    def __init__(self):
        pass

    def apply(self, message):
        # print(message)
        return message

class RawPricesProducer(Producer):
    """A producer class to bring in the stream, process it and produce a new stream. """
    def __init__(
        self, consumer:Consumer, 
        processor:Processor, config:Config,
        run_once: bool = False
    ):
        self.consumer = consumer
        self.processor = processor
        self.config = config
        self.producer = KafkaProducer(
            bootstrap_servers=self.config.bootstrap_server
        )
        self.run_once = run_once
        self.stream()
        
    def stream(self):
        continue_loop = True
        while continue_loop:
            message = self.consumer.poll()
            processed_message = self.processor.apply(message)
            self.producer.send(
                self.config.prices_topic, 
                json.dumps(processed_message).encode('utf-8')
            )

            if self.run_once: continue_loop = False

            if DELAY_FOR_DEV: time.sleep(1)


class RawPricing(StreamPipeline):
    """
    A concreate factory to run the stream pipelin for raw pricing end to end. 
    It has abstracted the process of consuming and transforming data from the 
    actual 'how' it is consumed and processed. 
    """

    def consume(self) -> RawPricesConsumer:
        return RawPricesConsumer()
    
    def process(self) -> RawPricesProcessor:
        return RawPricesProcessor()

    def produce(self) -> RawPricesProducer:
        return RawPricesProducer(
            consumer=self.consume(),
            processor=self.process(), 
            config=Config()
        )

if __name__ == '__main__':
    RawPricing().produce()