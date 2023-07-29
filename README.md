# Big Data Course Project- Yet Another Kafka

To run the project

Download the files from the github. The main branch is the one upto date with a working producer, consumer, 3 brokers and a mini-zookeeper.

To begin, first run
`python zookeeper.py`

This starts all 3 brokers and starts polling broker 1, broker 2, and broker 3 where broker 1 is the leader and communicates with broker 2 and broker 3 and the producers and consumers.

In another terminal, run
`python producer.py`

Multiple producers can run at the same time and and can publish to the same topic at once.

In a new terminal, run
`python consumer.py`

Multiple producers can run at the same time and can read from the same topic.
(Due to the order of starting of threads, for 2nd consumers to run, 2nd producer must also start for the broker to reach the consumer thread spawning)

For consumer to read from the time of subscribing to the broker- `read`

For consumer to read from the time of topic creation- `read --from-beginning`

Team:
[Karuna](https://github.com/karunakc)

[Harshit](https://github.com/Harshit-crypto)

[Lenver](https://github.com/lenverpinto)



