import json
import queue
from pprint import pprint

import paho.mqtt.client as mqtt

from nuvla.api import Api as Nuvla
from nuvla.api.models import CimiResource


class DataConsumer:
    def __init__(self, nuvla: Nuvla, topic: str, host: str, port: int):
        self.nuvla: Nuvla = nuvla
        self.topic: str = topic
        self.host = host
        self.port = port

        self.mqtt_client: mqtt.Client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        self.link_queue: queue.Queue = queue.Queue()

    def on_connect(self, cli, userdata, flags, rc, properties=None):

        if rc == 0:
            print("Connected to broker")
            self.mqtt_client.subscribe(self.topic)
        else:
            print("Connection failed")

    def _get_dr_from_message(self, message) -> CimiResource | None:
        t = json.loads(message.payload.decode())

        dr_id: str = t.get("resource_uri", None)
        if not dr_id or "api/data-record" not in dr_id:
            print("No resource URI in message")
            return None

        dr_id = dr_id.replace("api/", "")
        pprint("Data record ID: " + dr_id)
        return self.nuvla.get(dr_id)

    def on_message(self, cli, userdata, message):
        try:
            data_record = self._get_dr_from_message(message)
            data = {"data-record": data_record.data}
            if not data_record or not data_record.data:
                return

            data_object_id: str = data_record.data.get("data-object", None)

            if not data_object_id:
                print("No data object in data record")
                return

            data_object = self.nuvla.get(data_object_id)
            data["data-object"] = data_object.data
            data["link"] = self.nuvla.operation(data_object, "download").data

            self.link_queue.put(data)

        except Exception as e:
            print(f"Error processing message: {e}")

    def listen(self):
        """
        Listen to the data catalogue for new events
        """
        self.mqtt_client.connect(self.host, self.port)
        self.mqtt_client.loop_start()
