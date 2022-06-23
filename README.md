# kafka-example
Maven project generted with com.a9ski:quick-start archetype


# Development guide
1. Install pre-commit (https://pre-commit.com/)
2. Install the pr-commit hook by executing `pre-commit install` inside project directory
3. Run against all files in the project: `pre-commit run --all-files`

# Building the application

```
mvn clean install
```

# Running docker

The docker compose file is based on [kafka-ui docker examples](https://github.com/provectus/kafka-ui/blob/master/documentation/compose/kafka-ui.yaml) (only the port numbers are modified).

Startup docker compose with 

```
docker compose up -d
```

Stop and remove containers with

```
docker compose down
```

Accessing kafka-ui on [http://localhost:18080](http://localhost:18080)

# Running the application

This command line will send (and receive) 10 messages to topic 1 on broker 127.0.0.1:9092. Will timeout after 20 seconds.

```
mvn exec:java -Dexec.args="-m 10 --t topic1 -b 127.0.0.1:9092 -T 20"
```

or

Windows:

```
java -cp "target\KafkaTest-1.0.0-SNAPSHOT.jar;target\libs\*" com.a9ski.kafka.example.Application -m 10 --t topic1 -b 127.0.0.1:9092 -T 20
```

Linux:

```
java -cp "target/KafkaTest-1.0.0-SNAPSHOT.jar:target/libs/*" com.a9ski.kafka.example.Application -m 10 --t topic1 -b 127.0.0.1:9092 -T 20
```