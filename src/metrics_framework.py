from abc import ABC, abstractmethod
from config import Config


class Consumer(ABC):
    """
    An abstraction for all consumers in the system. 
    This is useful here as it allows for multiple 'consumer types'
    
    The code provided for 'raw prices' and the processed metrics from this data
    both have different consumer types.  I'v maintained the 'poll' method from the original script. 
    """
    @abstractmethod
    def poll(self):
        pass

class Processor(ABC):
    """
    A abstraction of a generic 'processing' class.
    In this instance used for moving averages and exponential moving averages. 
    This provides a generic step for any data processing in flight. 
    """
    @abstractmethod
    def apply(self):
        pass

class Producer(ABC):
    """
    Abstraction of producers. 
    """
    @abstractmethod
    def stream(self):
        pass


class StreamPipeline(ABC):
    """
    This is an abstraction that pull together a basic process for passing stream data
    along a chain of processes. 
    """
    @abstractmethod
    def consume(self) -> Consumer:
        pass
    
    @abstractmethod
    def process(self) -> Processor:
        pass

    @abstractmethod
    def produce(self) -> Producer:
        pass


class StreamArchiver(ABC):
    """
    This is an abstraction to consume and then store Topics. 
    Keeping it simple here, consume then log. 
    """

    @abstractmethod
    def consume(self) -> Consumer:
        pass

    @abstractmethod
    def store(self) -> None:
        pass