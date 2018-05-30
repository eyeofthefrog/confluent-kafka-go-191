FROM ubuntu:17.10
RUN apt-get update
RUN apt-get install -y librdkafka1
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
ADD traffic /
CMD ["/traffic"]
