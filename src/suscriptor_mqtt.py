import json
import time
import paho.mqtt.client as mqtt

BROKER = "ec2-54-221-117-7.compute-1.amazonaws.com"
PORT = 1883
USER = "omar"
PASS = "123456"
TOPIC = "sensores/co2/#"

def on_connect(client, userdata, flags, reason_code, properties):
    # reason_code puede ser un objeto; convertir a string ayuda a entender
    print("Conectado. reason_code:", reason_code)
    # si la conexión fue exitosa, subscribe:
    try:
        # Para MQTT v5 el subscribe puede ir aquí
        client.subscribe(TOPIC, qos=1)
        print("Suscrito a:", TOPIC)
    except Exception as e:
        print("Error al subscribir:", e)

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
    except Exception:
        data = msg.payload.decode(errors="ignore")
    print("Recibido:", msg.topic, data)

def on_disconnect(client, userdata, reason_code, properties):
    print("Desconectado. reason_code:", reason_code)

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect

client.username_pw_set(USER, PASS)

try:
    client.connect(BROKER, PORT, keepalive=60)
except Exception as e:
    print("Error al conectar al broker:", e)
    raise SystemExit(1)

client.loop_forever()
