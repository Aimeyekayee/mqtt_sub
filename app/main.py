import random
import asyncio

from dotenv import dotenv_values

from publish.accumulate import AccumulateETL
from manager.mqtt import MqttManager

config = dotenv_values(".env")


def sub_on_message_func(topic, payload):
    print(topic)
    if "rotor" in topic:
        subscribe.power_receive(topic, payload)

def sub_on_connect_func(manager):
    subscribe.set_sub_client(manager)


sub_topics = "263315ab48dd4982971f157cd97faa4a/rotor/linenotify"
sub_client_id = f"subscribe-{random.randint(0, 100)}"
sub_mqtt = MqttManager(
    broker="broker.emqx.io",
    port=8083,
    sub_topics=sub_topics,
    pub_topics=None,
    client_id=sub_client_id,
    on_connect_func=sub_on_connect_func,
    on_message_func=sub_on_message_func,
    name="subscriber",
)

subscribe = AccumulateETL(mqtt_client=sub_mqtt)


def main():
    sub_mqtt.initialize()


if __name__ == "__main__":
    main()
    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass
