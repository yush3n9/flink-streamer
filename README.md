# flink-streamer
A demo application which receives the real time message from kafka and do some data processing job on them and sink them into ElasticSearch finally.

# Why custom sinker?
Flink ElasticSearch Connector supports ElasticSearch only up to version 5.x, but I use 6.2.x. I implemented a custom sinker by using the official ElasticSearch Java API.

# Visualization
After/ During the data are written into ElasticSearch, they can be visualized in Kibana in real time.
![es_bitcoin](https://user-images.githubusercontent.com/39279696/40230836-54694470-5a99-11e8-8566-635951a8d794.png)
