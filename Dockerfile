FROM ubuntu

RUN apt-get update
RUN apt-get install -y python

ADD bin/ /pykettle/bin/
ADD lib/ /pykettle/lib/
ADD samples/ /pykettle/samples/
