from mqtt_as import MQTTClient
from mqtt_local import config
import uasyncio as asyncio
import dht, machine
from machine import Pin
#from time import sleep
import ujson as json
import binascii

#d es el sensor DHT11
d = dht.DHT11(machine.Pin(15))

#r es el rele
r=machine.Pin(16, machine.Pin.OUT)
r.value(1) #rele activo en bajo

#led es el led del la rasperry
led=machine.Pin("LED", machine.Pin.OUT)

#se encuentra el ID del dispositivo
ID_del_dispositivo = binascii.hexlify(machine.unique_id()).decode()
archivo_config="configuracion.json"

#Cargo la configuracion  o establce por defecto si no hay archivo
def cargar_config():
    try:
        with open(archivo_config, "r") as f:
            return json.load(f)
    except OSError:
        return {
            "setpoint": 25.0,
            "periodo": 5,
            "modo": "auto",  #Se utiliza "auto" no tener que escribir automatico 
            "rele": 1
        }
    
#guarda la modificaciones en el archivo json
def guardar_config(ajustes_datos):
    with open(archivo_config, "w") as archivo:
        json.dump(ajustes_datos, archivo)



async def conn_han(client):
    await client.subscribe(f"{ID_del_dispositivo}/setpoint", 1)
    await client.subscribe(f"{ID_del_dispositivo}/periodo", 1)
    await client.subscribe(f"{ID_del_dispositivo}/destello", 1)
    await client.subscribe(f"{ID_del_dispositivo}/modo", 1)
    await client.subscribe(f"{ID_del_dispositivo}/rele", 1)


evento_destello = asyncio.Event()

ajustes = cargar_config()

# procesa los mensajes recibidos y actualiza json si es que hubo modificaciones
async def procesar_mensajes(client):
    async for topic, msg, retained in client.queue:
        
            t = topic.decode()
            m = msg.decode()
            print(f"Mensaje: {t} -> {m}")

            ban=False
            if "setpoint" in t:
                ajustes["setpoint"] = float(m)
                ban=True
            if "periodo" in t:
                ajustes["periodo"] = int(m)
                ban=True
            if "modo" in t:
                m=m.lower()
                if m in ["auto", "manual"]:
                    ajustes["modo"] = m
                    ban=True
                else:
                     ban=False
            if "rele" in t:
                ajustes["rele"] = int(m)   #si se pone un 1 se apaga el rele y si se pone un 0 se enciende el rele es activo bajo
                ban=True
            if "destello" in t:
                evento_destello.set() 
                ban=True

            if ban == True:
                guardar_config(ajustes)

#logica del bucle de control para el caso de rele manual y la temperatura para el caso auto
async def bucle_control(client):
    while True:
        try:
            d.measure()
            t = d.temperature()
            h= d.humidity()

            # Lógica de control
            if ajustes["modo"] == "auto":
                if t > ajustes["setpoint"]:
                    r.value(0)
                else:
                    r.value(1)
            else: # Modo manual
                r.value(ajustes["rele"])

            # se publica el estado
            estado_datos= {
                "temperatura": t,
                "humedad": h,
                "setpoint": ajustes["setpoint"],
                "periodo": ajustes["periodo"],
                "modo": ajustes["modo"]
            }
            await client.publish(ID_del_dispositivo, json.dumps(estado_datos), qos=1)
            
        except Exception as e:
            print(f"Error al leer o publicar los datos: {e}")

        await asyncio.sleep(ajustes["periodo"])

async def tarea_destello():
    while True:
        await evento_destello.wait()
        for _ in range(20):
            led.toggle()
            await asyncio.sleep_ms(200)
        led.value(0)
        evento_destello.clear()


#configuracion de MQTT y del cliente
config['queue_len'] = 10
config['ssl'] = True
MQTTClient.DEBUG = True  


async def main():
    
    print("ID del dispositivo:", ID_del_dispositivo)
    client = MQTTClient(config)
    await client.connect()

    await conn_han(client)
    
    # se ejecutan las tareas de manera concurrente
    await asyncio.gather(
            procesar_mensajes(client),
            bucle_control(client),
            tarea_destello()
        )

try:
    asyncio.run(main())

except KeyboardInterrupt:
    r.value(1) #se interrumpe el programa se apaga el rele 
finally:
    asyncio.new_event_loop()


