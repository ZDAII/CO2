import time, json, random
import paho.mqtt.client as mqtt
from datetime import datetime

BROKER = "100.65.172.92"
PORT = 1883
TOPIC = "sensores/co2/sala1"
USER = "omar"
PASS = "123456"

def generar_lectura():
    base = 450 + random.gauss(0, 20)
    cambio = random.uniform(-5, 10)
    valor = max(300, base + cambio + random.uniform(-50,50))
    return round(valor, 1)

def payload():
    return json.dumps({
        "sensor_id": "sim_co2_01",
        "ts": datetime.utcnow().isoformat() + "Z",
        "co2_ppm": generar_lectura(),
        "battery": round(random.uniform(3.6, 4.2), 2)
    })

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.connect(BROKER, PORT, 60)
client.loop_start()

try:
    while True:
        msg = payload()
        client.publish(TOPIC, msg, qos=1)
        print("Publicado:", msg)
        time.sleep(5)  # frecuencia de envío (segundos)
except KeyboardInterrupt:
    client.loop_stop()
    client.disconnect()
