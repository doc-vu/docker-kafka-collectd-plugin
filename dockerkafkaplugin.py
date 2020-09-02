#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import pickle
import socket
import threading
import time
from jmxquery import JMXConnection, JMXQuery
import collectd
import subprocess
import socket
import docker


def get_cnr_ip(con_id):
    cnr_pid = subprocess.check_output("docker inspect --format '{{ .State.Pid }}' %s" % con_id,
                                      shell=True).decode().strip()
    cnr_ip = subprocess.check_output("sudo nsenter -t %s -n ip addr | grep inet | awk '{print $2}' | tail -n 1" % cnr_pid,
                            shell=True).decode().strip().split('/')[0]
    return cnr_ip


def get_cluster_ip(con_id):
    try:
        cnr_env_vars = subprocess.check_output("docker inspect --format '{{ json .Config.Env }}' %s" % con_id,
                                               shell=True).decode()
        cnr_env_vars = json.loads(cnr_env_vars)
        for var in cnr_env_vars:
            # the output string is in formate: "cluster_ip=xx.xx.xx.xx"
            if 'cluster_ip' in var:
                cluster_ip = var.split('=')[1]
                return cluster_ip
    except:
        return get_cnr_ip(con_id)


def get_node_forward_addr(con_id, cnr_port):
    host_ip = socket.gethostbyname(socket.gethostname())
    try:
        # K8s
        cnr_labels = subprocess.check_output("docker inspect --format '{{ json .Config.Labels }}' %s" % con_id,
                                               shell=True).decode()
        cnr_labels = json.loads(cnr_labels)
        for label in cnr_labels:
            if 'annotation.io.kubernetes.container.ports' in label:
                for ports in label:
                    if 'hostPort' in list(ports.keys()):
                        return host_ip + ':' + ports['hostPort']

        # Docker
        cnr_ports = subprocess.check_output("docker inspect --format '{{ json .NetworkSettings.Ports }}' %s" % con_id,
                                            shell=True).decode()
        cnr_ports = json.loads(cnr_ports)
        for cp in cnr_ports:
            if cp.split('/')[0] == str(cnr_port):
                return host_ip + ':' + cnr_ports[cp][0]['HostPort']
    except:
        return get_cnr_ip(con_id) + ':' + str(cnr_port)


def locate_kafka_cnr(connection='host_ip', port=9999):
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
            if connection == 'container_ip':
                kafka_cons.append((con.attrs['Name'].replace('/', ''), get_cnr_ip(con.short_id) + ':' + str(port)))
            elif connection == 'cluster_ip':
                kafka_cons.append((con.attrs['Name'].replace('/', ''), get_cluster_ip(con.short_id) + ':' + str(port)))
            else:
                kafka_cons.append((con.attrs['Name'].replace('/', ''), get_node_forward_addr(con.short_id, port)))

    return kafka_cons


class QueryHelper(threading.Thread):
    def __init__(self, func, args=()):
        super(QueryHelper, self).__init__()
        self.func = func
        self.args = args
        self.result = self.func(*self.args)

    def get_result(self):
        try:
            return self.result
        except Exception as ex:
            collectd.warning(ex.message)
            return None


class DockerKafkaMon(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self._stop_event = threading.Event()
        self.plugin_name = 'docker_kafka'
        self.interval = 5
        self.hostname = socket.gethostname()
        self.verbose_logging = False
        self.tmp_out = None
        self.mbeans_file = None
        self.connection = 'host_ip'
        self.jmx_port = 9999

    def log_verbose(self, msg):
        if not self.verbose_logging:
            return
        collectd.info('%s plugin [verbose]: %s' % (self.plugin_name, msg))

    def configure_callback(self, conf):
        for node in conf.children:
            val = str(node.values[0])
            if node.key == "HostName":
                self.hostname = val
            elif node.key == 'Interval':
                self.interval = int(float(val))
            elif node.key == 'Verbose':
                self.verbose_logging = val in ['True', 'true']
            elif node.key == 'PluginName':
                self.plugin_name = val
            elif node.key == 'TempOutputDir':
                self.tmp_out = val
            elif node.key == 'MbeansFile':
                self.mbeans_file = val
            elif node.key == 'Connection':
                self.connection = val
            elif node.key == 'JMX_PORT':
                self.jmx_port = int(val)
            else:
                collectd.warning('[plugin] %s: unknown config key: %s' % (self.plugin_name, node.key))

    def stop(self):
        collectd.info('Stopping the monitor thread...')
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.isSet()

    def run(self):
        with open(self.mbeans_file) as f:
            mbeans = json.load(f)
        query_obj = []
        for mbean in mbeans:
            for val in mbean['Values']:
                query_obj.append(JMXQuery(mBeanName=mbean['ObjectName'],
                                          attribute=val['Attribute'],
                                          value_type=val['Type'],
                                          metric_name=val['InstancePrefix'],
                                          metric_labels={'type': val['Type']}))

        while not self.stopped():
            jmx_conns = {}
            active_kafka_cons = locate_kafka_cnr(self.connection, self.jmx_port)
            for kc in active_kafka_cons:
                svc_url = 'service:jmx:rmi:///jndi/rmi://%s/jmxrmi' % kc[1]
                jmx_conns.update({kc[0]: JMXConnection(svc_url)})
            if len(jmx_conns) == 0:
                continue
            threads = [QueryHelper(jmx_conns[cnr].query, args=(query_obj, self.interval, )) for cnr in jmx_conns]
            [thr.start() for thr in threads]
            query_results = {}
            cnrs = list(jmx_conns.keys())
            for i, cnr in enumerate(cnrs):
                query_results.update({cnr: threads[i].get_result()})
            with open(self.tmp_out + '/tmp.out', 'w') as f:
                pickle.dump(query_results, f)

    def init_callback(self):
        self.start()

    def dispatch_value(self, plugin, plugin_instance, host, type, type_instance, value):
        self.log_verbose("Dispatching value plugin=%s, host=%s, type=%s, type_instance=%s, value=%s" %
                         (plugin, host, type, type_instance, str(value)))
        val = collectd.Values(type=type)
        val.plugin = plugin
        val.host = host
        val.type_instance = type_instance
        val.plugin_instance = plugin_instance
        val.interval = self.interval
        val.time = time.time()
        val.values = [value]
        val.dispatch()
        self.log_verbose("Dispatched value plugin=%s, host=%s, type=%s, type_instance=%s, value=%s" %
                         (plugin, host, type, type_instance, str(value)))

    def read_callback(self):
        failures = 0
        try:
            with open(self.tmp_out + '/tmp.out') as f:
                results = pickle.load(f)
            for cnr in results:
                for mtr in results[cnr]:
                    self.dispatch_value(plugin=self.plugin_name, plugin_instance=cnr, host=self.hostname,
                                        type=mtr.metric_labels['type'], type_instance=mtr.metric_name, value=mtr.value)
        except Exception as ex:
            time.sleep(1)
            failures += 1
            if failures > 3:
                collectd.error(('Unable to read docker kafka stats from %s: %s' % (self.hostname, ex)))
                self.stop()

    def shutdown_callback(self):
        self.stop()


dk_mon = DockerKafkaMon()
collectd.register_init(dk_mon.init_callback)
collectd.register_config(dk_mon.configure_callback)
collectd.register_read(dk_mon.read_callback)
collectd.register_shutdown(dk_mon.shutdown_callback)
