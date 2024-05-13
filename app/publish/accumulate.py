import json
from datetime import datetime

from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import text
from typing import Optional

from manager.mqtt import MqttManager


class AccumulateETL:
    def __init__(
        self,
        mqtt_client: MqttManager,

    ):
        self.mqtt_client = mqtt_client

    def set_pub_client(self, client):
        self.pub_client = client

    def set_sub_client(self, client):
        self.sub_client = client
        print("set_sub_client: ", client)

    def power_receive(self, topic: str, payload: str):
            # print("print power_receive linkage", self.linkage)
            keys = topic.split("/")
            data = json.loads(payload)
            print(data)
