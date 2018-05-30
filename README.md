# confluent-kafka-go-191

This code will pull the Kafka 1.0.0 docker image, start the container, and recreate an issue similar to https://github.com/confluentinc/confluent-kafka-go/issues/191

To run the example:
```
go get github.com/eyeofthefrog/confluent-kafka-go-191
cd ~/go/src/github.com/eyeofthefrog/confluent-kafka-go-191
go get ./...
go build
./confluent-kafka-go-191
docker run --network=issue-191 -it --rm eyeofthefrog/confluent-kafka-go-191 /traffic
```
