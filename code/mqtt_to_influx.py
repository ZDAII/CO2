import json
from datetime import datetime
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point, WritePrecision

BROKER = "100.65.172.92"
PORT = 1883
USER = "omar"
PASS = "123456"
TOPIC = "sensores/co2/#"

INFLUX_URL = "http://localhost:8086"
INFLUX_TOKEN = "M1niBVyLyojwlj0qOIT2B2vEj1SAp-UBii1Ywx63m5v6KGgu8VLYCzqVZezx3YU-28mly4p8gRRsLSUX6Xhqgg=="
INFLUX_ORG = "SistemasProgramables"
INFLUX_BUCKET = "sensores"

client_influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = client_influx.write_api()

def on_connect(client, userdata, flags, rc):
    print("Conectado al MQTT, result code:", rc)
    client.subscribe(TOPIC, qos=1)

def on_message(client, userdata, msg):
    try:
        j = json.loads(msg.payload.decode())
    except Exception:
        print("Payload no JSON:", msg.payload)
        return

    # ejemplo: convertir a punto de Influx
    p = Point("co2_sensor") \
        .tag("sensor_id", j.get("sensor_id","unknown")) \
        .field("co2_ppm", float(j.get("co2_ppm", 0))) \
        .field("battery", float(j.get("battery", 0)))

    # tiempo (si envías ts en ISO8601, parsea; aquí uso tiempo recibido)
    if "ts" in j:
        try:
            t = datetime.fromisoformat(j["ts"].replace("Z", "+00:00"))
            p = p.time(t, WritePrecision.NS)
        except Exception:
            pass

    write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=p)
    print("Escrito a Influx:", p)

mqttc = mqtt.Client()
mqttc.username_pw_set(USER, PASS)
mqttc.on_connect = on_connect
mqttc.on_message = on_message
mqttc.connect(BROKER, PORT, 60)
mqttc.loop_forever()
