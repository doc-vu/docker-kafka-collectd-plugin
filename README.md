# docker-kafka-collectd-plugin
A Collectd plugin for monitoring dockerized Kafka servers running on a host. 

If Kafka container listens on container IP, we capture the address by inspecting its network space; but if the container(pod) associates with a Kubernetes Service(ClusterIP), we explicity write the "cluster_ip" as an environment variable when creating the container.
