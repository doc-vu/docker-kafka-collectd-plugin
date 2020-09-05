#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import socket
import sys
import threading
import time
from jmxquery import JMXConnection, JMXQuery
import subprocess
import docker


def get_cnr_ip(con_id):
    cnr_pid = subprocess.check_output("docker inspect --format '{{ .State.Pid }}' %s" % con_id,
                                      shell=True).decode().strip()
    cnr_ip = \
    subprocess.check_output("sudo nsenter -t %s -n ip addr | grep inet | awk '{print $2}' | tail -n 1" % cnr_pid,
                            shell=True).decode().strip().split('/')[0]
    return cnr_ip


def locate_kafka_cnr(port=9999):
    docker_api = docker.from_env()
    cnrs = docker_api.containers.list()
    kafka_cons = []
    for con in cnrs:
        check_jvm = "docker exec %s lsof -i :%d | awk '{print $2}' | tail -n 1" % (con.short_id, port)
        try:
            int(subprocess.check_output(check_jvm, shell=True).decode())
        except Exception as ex:
            continue
        else:
            kafka_cons.append((con.attrs['Name'].replace('/', ''), get_cnr_ip(con.short_id) + ':' + str(port)))
    return kafka_cons


class JMXMetrics(threading.Thread):
    def __init__(self, jmx_info, mbenas_file):
        super(JMXMetrics, self).__init__()
        self.stop = False
        svc_url = 'service:jmx:rmi:///jndi/rmi://%s/jmxrmi' % jmx_info[1]
        self._conn = JMXConnection(svc_url)
        self._cnr_name = jmx_info[0]
        self._metrics = None

        with open(mbenas_file) as f:
            mbeans = json.load(f)
        self._query_obj = []
        for mbean in mbeans:
            for val in mbean['Values']:
                self._query_obj.append(JMXQuery(mBeanName=mbean['ObjectName'],
                                                attribute=val['Attribute'],
                                                value_type=val['Type'],
                                                metric_name=val['InstancePrefix'],
                                                metric_labels={'type': val['Type']}))

        # Automatically start stats reading thread
        self.start()

    def run(self):
        logger.info('start gathering metrics for Kafka container: %s' % self._cnr_name)
        failures = 0
        while not self.stop:
            try:
                self._metrics = self._conn.query(self._query_obj)
            except Exception as ex:
                failures += 1
                if failures >= 3:
                    self.stop = True
                logger.error('failed to query metrics for kafka container %s' % self._cnr_name)
        logger.info('stop gathering metrics for kafka container %s' % self._cnr_name)

    @property
    def metrics(self):
        if self._metrics:
            return self._metrics
        return None


def dispatch_value(plugin, plugin_instance, host, type, type_instance, value):
    val = collectd.Values(type=type)
    val.plugin = plugin
    val.host = host
    val.type_instance = type_instance
    val.plugin_instance = plugin_instance
    val.time = time.time()
    val.values = [value]
    val.dispatch()
    logger.info("Dispatched value plugin=%s, host=%s, type=%s, type_instance=%s, value=%s" %
                     (plugin, host, type, type_instance, str(value)))


class DockerKafkaPlugin:
    def __init__(self):
        self.plugin_name = 'docker_kafka'
        self.interval = 5
        self.hostname = socket.gethostname()
        self.mbeans_file = None
        self.jmx_port = 9999
        self.jmx_threads = {}

    def configure_callback(self, conf):
        for node in conf.children:
            val = str(node.values[0])
            if node.key == "HostName":
                self.hostname = val
            elif node.key == 'Interval':
                self.interval = int(float(val))
            elif node.key == 'PluginName':
                self.plugin_name = val
            elif node.key == 'MbeansFile':
                self.mbeans_file = val
            elif node.key == 'JMX_PORT':
                self.jmx_port = int(val)
            else:
                logger.warning('[plugin] %s: unknown config key: %s' % (self.plugin_name, node.key))

    def read_callback(self):
        active_kafka_cons = locate_kafka_cnr(self.jmx_port)

        # Terminate stats gathering threads for containers that are not running anymore.
        for cnr_name in set(self.jmx_threads) - set(map(lambda kc: kc[0], active_kafka_cons)):
            # Log each container that is stopped
            self.jmx_threads[cnr_name].stop = True
            logger.info('stopping stats gathering for %s' % cnr_name)
            del self.jmx_threads[cnr_name]

        for kc in active_kafka_cons:
            if kc[0] not in self.jmx_threads:
                self.jmx_threads.update({kc[0]: JMXMetrics(kc, self.mbeans_file)})
            jmx_thr = self.jmx_threads[kc[0]]
            metrics = jmx_thr.metrics

            # not ready for read
            if not metrics:
                continue

            try:
                for mtr in metrics:
                    dispatch_value(plugin=self.plugin_name, plugin_instance=kc[0], host=self.hostname,
                                   type=mtr.metric_labels['type'], type_instance=mtr.metric_name, value=mtr.value)
            except Exception as ex:
                logger.error('failed to dispatch metric for %s' % kc[0])

    def init_callback(self):
        collectd.register_read(self.read_callback, interval=self.interval)

    def shutdown_callback(self):
        for cnr in self.jmx_threads:
            self.jmx_threads[cnr].stop = True


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
# handler = logging.FileHandler('/usr/share/collectd/docker-kafka-collectd-plugin')
# handler.setFormatter(formatter)
# logger.addHandler(handler)


if __name__ == '__main__':
    plugin = DockerKafkaPlugin()
    while True:
        plugin.read_callback()
else:
    import collectd

    plugin = DockerKafkaPlugin()
    collectd.register_config(plugin.configure_callback)
    collectd.register_init(plugin.init_callback)
    collectd.register_shutdown(plugin.shutdown_callback)
