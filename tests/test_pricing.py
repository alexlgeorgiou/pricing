from src.expo_moving_avg import (
    PricesConsumer, PricesProducer, ExpMovingAvgProcessor, ExponentialMovingAverage
)

from kafka import KafkaProducer

from unittest.mock import patch

class MockProducer:
    def send(self, *args, **kwargs):
        pass

class NewPricesProducer(PricesProducer):
    def __init__(self, 
        consumer,
        processor,
        topic, 
        bootstrap_server,
        run_once
    ): 
        self.consumer = consumer
        self.processor = processor
        self.topic = topic
        self.producer = MockProducer()
        self.run_once = True

@patch('src.expo_moving_avg.ExpMovingAvgProcessor')
@patch('src.expo_moving_avg.PricesConsumer', autospec=True)
def test_price_producer(mock_price_consumer, mock_ema_processor):

    mock_price_consumer.poll.return_value = ['a', 'b']
    mock_ema_processor.apply.return_value = 'msg'

    test_producer = NewPricesProducer(
        mock_price_consumer,
        mock_ema_processor,
        'topic',
        'bs_server',
        run_once=True
    )

    test_producer.stream()

    assert mock_price_consumer.poll.call_count == 1
    assert mock_ema_processor.apply.call_count == 2
    
