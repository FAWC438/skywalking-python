#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import ast
import os


from skywalking import config
from skywalking.client import MeterReportService, ServiceManagementClient, TraceSegmentReportService, LogDataReportService
from skywalking.loggings import logger, logger_debug_enabled
from skywalking.protocol.language_agent.Meter_pb2 import MeterDataCollection
from skywalking.protocol.management.Management_pb2 import InstancePingPkg, InstanceProperties

kafka_configs = {}
confluent_kafka_enabled = False


def __init_kafka_configs():
    global confluent_kafka_enabled
    prefork_server_detected = os.getenv('prefork')
    if prefork_server_detected == 'gunicorn' or not prefork_server_detected:
        # only guniconn or non-prefork model can use confluent_kafka
        confluent_kafka_enabled = True

    if confluent_kafka_enabled:
        kafka_configs['bootstrap.servers'] = config.kafka_bootstrap_servers
    else:
        kafka_configs['bootstrap_servers'] = config.kafka_bootstrap_servers.split(',')

    # process all kafka configs in env
    kafka_keys = [key for key in os.environ.keys() if key.startswith('SW_KAFKA_REPORTER_CONFIG_')]
    for kafka_key in kafka_keys:
        key = kafka_key[25:]
        if confluent_kafka_enabled:
            # Although the content of configs is basically the same
            # - kafka-python: use '_' to separate config words
            # - confluent-kafka-python: use '.' to separate config words
            key = key.replace('_', '.')
        val = os.environ.get(kafka_key)

        if val is not None:
            if val.isnumeric():
                val = int(val)
            elif val in ['True', 'False']:
                val = ast.literal_eval(val)
            elif confluent_kafka_enabled:
                val = val.replace('_', '.')
        else:
            continue

        # check if the key was already set
        if kafka_configs.get(key) is None:
            kafka_configs[key] = val
        else:
            raise KafkaConfigDuplicated(key)


__init_kafka_configs()


class KafkaProducerClient():
    def __init__(self):
        if confluent_kafka_enabled:
            from confluent_kafka import Producer
            self.producer_client = Producer(kafka_configs)
        else:
            from kafka import KafkaProducer
            self.producer_client = KafkaProducer(**kafka_configs)

    def send(self, topic: str, key=None, value=None, callback=None):
        if confluent_kafka_enabled:
            # poll() can be used to trigger delivery report callbacks
            # but it can also clearn up the internal queue without any performance impact
            # see https://github.com/confluentinc/confluent-kafka-dotnet/issues/703#issuecomment-573777022
            try:
                self.producer_client.produce(topic=topic, key=key, value=value, on_delivery=callback)
                self.producer_client.poll(0)
            except BufferError:
                logger.warn('kafka producer Buffer full, waiting for free space on the queue')
                self.producer_client.poll(10)
                self.producer_client.produce(topic=topic, key=key, value=value, on_delivery=callback)
            return None
        else:
            future = self.producer_client.send(topic=topic, key=key, value=value)
            return future

    def clean_queue(self):
        """Clean up the internal queue, only for confluent_kafka_enabled"""
        if confluent_kafka_enabled:
            self.producer_client.flush()


class KafkaServiceManagementClient(ServiceManagementClient):
    def __init__(self):
        super().__init__()
        self.instance_properties = self.get_instance_properties_proto()

        if logger_debug_enabled:
            logger.debug('Kafka reporter configs: %s', kafka_configs)
        self.producer = KafkaProducerClient()
        self.topic_key_register = 'register-'
        self.topic = config.kafka_topic_management

        self.send_instance_props()

    def send_instance_props(self):
        instance = InstanceProperties(
            service=config.agent_name,
            serviceInstance=config.agent_instance_name,
            properties=self.instance_properties,
        )

        key = bytes(self.topic_key_register + instance.serviceInstance, encoding='utf-8')
        value = instance.SerializeToString()
        self.producer.send(topic=self.topic, key=key, value=value)

    def send_heart_beat(self):
        self.refresh_instance_props()

        if logger_debug_enabled:
            logger.debug(
                'service heart beats, [%s], [%s]',
                config.agent_name,
                config.agent_instance_name,
            )

        instance_ping_pkg = InstancePingPkg(
            service=config.agent_name,
            serviceInstance=config.agent_instance_name,
        )

        key = bytes(instance_ping_pkg.serviceInstance, encoding='utf-8')
        value = instance_ping_pkg.SerializeToString()

        if confluent_kafka_enabled:
            def heartbeat_callback(err, msg):
                if err:
                    logger.error('heartbeat error: %s', err)
                else:
                    logger.debug('heartbeat response: %s', msg)
            self.producer.send(topic=self.topic, key=key, value=value, callback=heartbeat_callback)
        else:
            future = self.producer.send(topic=self.topic, key=key, value=value)
            res = future.get(timeout=10)
            if logger_debug_enabled:
                logger.debug('heartbeat response: %s', res)


class KafkaTraceSegmentReportService(TraceSegmentReportService):
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.topic = config.kafka_topic_segment

    def report(self, generator):
        for segment in generator:
            key = bytes(segment.traceSegmentId, encoding='utf-8')
            value = segment.SerializeToString()
            self.producer.send(topic=self.topic, key=key, value=value)


class KafkaLogDataReportService(LogDataReportService):
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.topic = config.kafka_topic_log

    def report(self, generator):
        for log_data in generator:
            key = bytes(log_data.traceContext.traceSegmentId, encoding='utf-8')
            value = log_data.SerializeToString()
            self.producer.send(topic=self.topic, key=key, value=value)


class KafkaMeterDataReportService(MeterReportService):
    def __init__(self):
        self.producer = KafkaProducerClient()
        self.topic = config.kafka_topic_meter

    def report(self, generator):
        collection = MeterDataCollection()
        collection.meterData.extend(list(generator))
        key = bytes(config.agent_instance_name, encoding='utf-8')
        value = collection.SerializeToString()
        self.producer.send(topic=self.topic, key=key, value=value)


class KafkaConfigDuplicated(Exception):
    def __init__(self, key):
        self.key = key
