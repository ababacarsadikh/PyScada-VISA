# -*- coding: utf-8 -*-
from __future__ import unicode_literals

import paho.mqtt.client as mqtt
from pyscada.device import GenericDevice
from .devices import GenericDevice as GenericHandlerDevice


try:
    import paho.mqtt.client as mqtt_client

    driver_ok = True
except ImportError:
    driver_ok = False

from time import time
import logging

logger = logging.getLogger(__name__)


class Device(GenericDevice):
    def __init__(self, device):
        self.driver_ok = driver_ok
        self.handler_class = GenericHandlerDevice
        super().__init__(device)

        self._address = device.mqttbroker.address
        self._port = device.mqttbroker.port
        self._timeout = device.mqttbroker.timeout
        
        self.mqtt_client = None
        self.variables = {}

        for var in self.device.variable_set.filter(active=1):
            if not hasattr(var, "mqttvariable"):
                continue
            self.variables[var.pk] = var

        if self.driver_ok and self.driver_handler_ok:
            self._h.connect()
        else:
            logger.warning(f"Cannot import mqtt or handler for {self.device}")

    def write_data(self, topic, value, task):
        """
        write value to the instrument/device
        """
        output = []
        if not self.driver_ok:
            logger.info("Cannot import MQTT client")
            return output

        for item in self.variables.values():
            if not (item.mqttvariable.variable_type == 0 and item.id == variable_id):
                continue
            
            topic = item.mqttvariable.topic
            self._h.publish(topic, value)
            logger.info(f"Published to {topic}: {value}")
            output.append(item)


    def request_data(self):
        """
        request data from the instrument/device
        """
        
        output = []
        keys_to_reset = []
        for variable_id, variable in self.variables.items():
            if self.data.get(variable.mqttvariable.topic) is not None:
                value = self.data[variable.mqttvariable.topic].decode("utf-8")
                value = variable.mqttvariable.parse_value(value)
                timestamp = time()
                
                if variable.mqttvariable.timestamp_topic:
                    if self.data.get(variable.mqttvariable.timestamp_topic) is None:
                        logger.debug("MQTT request_data timestamp_topic is None")
                        continue
                    timestamp = self.data[variable.mqttvariable.timestamp_topic].decode("utf-8")
                    timestamp = variable.mqttvariable.parse_timestamp(timestamp)
                    keys_to_reset.append(variable.mqttvariable.timestamp_topic)
                
                self.data[variable.mqttvariable.topic] = None

                if variable.update_values([value], [timestamp]):
                    output.append(variable)
        
        for key in keys_to_reset:
            self.data[key] = None
        
        return output
