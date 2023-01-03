## Streaming Securities Example

### Some general context
* I've not worked with Kafka before and as such it has been a learning experience to put this together. I've enjoyed this, however do feel here is a lot more for me to learn to be happy with this solution.
* I've used the Abstract Factory design pattern.
* If I'm honest this is not ready to head into production for a few reasons I'll specify a little further down. 

## A quick overview of the structure of the project
#### Architecture
![](img/architecture.png 'title text')
### /src
This is where all of the code for services is stored. I've assumed one container would be used to run one 'stream'. I.e. say these we're packages up into a container. Then a single container could run say pricing.py as a service. Either on Kubernetes or some serverless container orchestrator in the cloud. I would normally have included a Dockerfile here, however I'm not able to get Docker working on Windows Home Edition so have chosen to not include it for now. 

### /src/config.py
Used for the services to have and an interface into config.yaml where settings like topics and bootstrap servers can be set.

### /src/metrics_framework.py
A collection of abstract factory classes and their 'product' classes. This was a key element of the solution design to generalise the 'get stream data', 'transform stream data', 'produce stream data' work flow.

### /src/pricing.py
Contains the code to take the PriceConsumer class provided and get it to produce to a 'prices' topic to be consumed by the exponential moving average stream. 

### /src/expo_moving_avg.py
Contains the code to consume the prices topic, process it and produce moving averages and exponential moving averages into a topic named 'volatility'.

### /src/archive.py
This is the code to consume any stream and can be used to store each message locally. This is clearly too simple of an implementation for production. The messages could be stored in a nosql db, or blob storage in the cloud for later processing. It's also not a scalable solution as 'write' speed would become an issue. One solution he may be batch writes either from memory or the topic archive. 

#### /kafka_2.13-3.3.1
This is where I'm running zookeeper and the kafka services from. You'll need to download and unzip this from [here](https://kafka.apache.org/quickstart)


## Must haves:
* Exponential Moving Average (EMA) - Window size 100 points: <span style="color:green">Implemented</span>
* Moving Volatility - Window size 100 points:  <span style="color:green">Implemented</span>
* Real time app calculating the above:  <span style="color:green">Implemented</span>
* Store the data (idempotent):<span style="color:red"> Not implemented.</span> I realised too late that I should have used confluent-kafka sdk to support idempotentcy, not kafka-python. I would use this next time however did not have the time to implement. 
* Consider scaling to multiple securities: <span style="color:green">Suggestions below.</span> 
    * More granular configuration management
    * Build using Docker for deployment to Kubernetes
    * Migrate to confluent kafka package for idempotency

### Some other limitations and areas for improvement:
* Ensuring each of the producers, streams or storers could be run independently on their own containers would be ideal here. However, my Windows machine (Home edition) won't work with Docker so I've not done this. (I'll need to buy a Mac)
* Data validation: clearly very important. Ideal world there would be data validators monitoring the topics and the 'processors' of data to ensure data contract compliance, anomaly detection and linked to operational systems. 
* Unit tests: II've created only one example tests here but will come back to this when I have more time. The unit test I've created is not optimal and would benefit from comprehensive fixtures and better mocking. It is also clear that a small refactor of my code is necessary mocking expo_moving_avg:line49 proved problematic. This is a problem with my design and can certainly be improved. 
* Other areas of improvement would be better logging/ telemetry what's occurring in the streams and exception handling.
* Workflow - I definitely needed a lot more terminal's open than I would normally have open for developing batch jobs. Without knowing the answer I'm sure there is some set-up that facilitates multiple terminals more elegantly. 

## Running the project

### Initialise the project
```console
git clone https://github.com/alexlgeorgiou/pricing.git
cd pricing
poetry install
```

### How to get everything running
Ensure you have kafka downloaded to this directory. 
Ensure you have Java installed. 
```console
# TERMINAL 1: Start the ZooKeeper service
cd kafka_2.13-3.3.1
bin/zookeeper-server-start.sh config/zookeeper.properties

# TERMINAL 2: Start the Kafka broker service
cd kafka_2.13-3.3.1
bin/kafka-server-start.sh config/server.properties

# TERMINAL 3: Create topics
cd kafka_2.13-3.3.1
bin/kafka-topics.sh --create --topic prices --bootstrap-server localhost:9092
bin/kafka-topics.sh --create --topic volatility --bootstrap-server localhost:9092

# TERMINAL 3: Listen to events
cd kafka_2.13-3.3.1
bin/kafka-console-consumer.sh --topic prices --bootstrap-server localhost:9092

# TERMINAL 4: Listen to events
cd kafka_2.13-3.3.1
bin/kafka-console-consumer.sh --topic volatility --bootstrap-server localhost:9092

# TERMINAL 5: Kick off Archiver
poetry run python src/archive.py --topic=prices
poetry run python src/archive.py --topic=volatility

# TERMINAL 6: Produce prices
poetry run python src/pricing.py

# TERMINAL 7: Produce prices
poetry run python src/expo_moving_avg.py
```