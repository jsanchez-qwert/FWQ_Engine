# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
"""
    TODO PREGUNTAR AL PROFESOR POR EL LOGIN
"""
import time
import traceback
from sys import argv
import threading
import socket
import kafka
from kafka import KafkaConsumer, KafkaProducer
import sqlite3


class LectorMovimientos(threading.Thread):
    def __init__(self, ip, port):
        self.ip_kafka = ip
        self.port_kakfa = port
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self):
        self.stop_event.set()

    def consumir(self, consumer,producer):

        # formato del mensaje de kafka
        # alias:[n,m]
        # alias:movimiento
        # los movimientos pueden ser NN SS EE WW NE SE NW SW
        movimiento = [0, 0]
        global VISITANTES
        global MAPA
        while not self.stop_event.is_set():
            for msg in consumer:
                print("recibido movimiento: " + msg)
                producer.send('mapas',MAPA)

    def run(self):
        print("INICIO LectorSensores")
        try:
            consumer = KafkaConsumer(bootstrap_servers=f'{self.ip_kafka}:{self.port_kakfa}',
                                     auto_offset_reset='earliest',
                                     consumer_timeout_ms=500)
            consumer.subscribe(['movimientos'])
            producer = KafkaProducer(bootstrap_servers=f'{self.ip_kafka}:{self.port_kakfa}')
            self.consumir(consumer,producer)
        except Exception as e:
            print("ERROR EN LectorSensores :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            print("FIN LectorMovimientos")


class Engine:
    def __init__(self, ip_k: str, port_k: int, max_visitantes: int, ip_w: str, port_w: int):
        self.ip_k = ip_k
        self.port_k = port_k
        self.max_visitantes = max_visitantes
        self.ip_w = ip_w
        self.port_w = port_w


class PideTiempos(threading.Thread):
    """
    Clase Thread que se conecta cada 3 segundos al servidor de tiempos que le
    hayamos indicado.
    """

    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port

    def stop(self):
        self.stop_event.set()

    def escibe(self, tiempos: dict) -> dict:
        """
        TODO metodotemporal, hacer esto en la base de datos
        """
        global TIEMPOS
        TIEMPOS = str(tiempos)

    def run(self):
        HEADER = 10

        while not self.stop_event.is_set():
            cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print(self.ip, self.port)
            cliente.connect((self.ip, self.port))
            print('a')
            print("conectado 1")
            size = int(cliente.recv(HEADER))
            print("size: ", size)
            tiempos = cliente.recv(size)
            print(f"he recibido {tiempos}")
            tiempos = eval(tiempos)
            self.escibe(tiempos)
            cliente.close()
            time.sleep(3)
        print("AAAAAAAAAAA")


def login(credenciales):
    """
    Se comunica con la base de datos para ver si las credecniasles son correctas
    TODO hacel la fucnion
    :param credenciales:
    :return:
    """
    return True


def hilo_login(conn, addr):
    """
    Este thread recibe todos los mensajes del topic (login_cliente)
    :param conn:
    :param addr:
    :return:
    """
    credenciales = {"alias": "", "passwd": ""}


def print_hi(name):
    # Use a breakpoint in the code line below to debug your script.
    print(f'Hi, {name}')  # Press Ctrl+F8 to toggle the breakpoint.


def control_datos_entrada(ip_k: str, port_k: int, max_visitantes: int, ip_w: str, port_w: int) -> bool:
    """
    Controla que los datos esten en formato correcto
    TODO hacer la funcion
    :param ip_k:
    :param port_k:
    :param max_visitantes:
    :param ip_w:
    :param port_w:
    :return:
    """
    return True


def manejador(signum, frame):
    print("Holas")


def cargaMapa():
    """
    con = sqlite3.connect("../database.db")
    cur = con.cursor()
    mapa = cur.execute("select mapa from mapa")
    return mapa
    """
    return [[b'' for i in range(0, 20)] for j in range(0, 20)]


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """
    if len(argv) < 6:
        print("Argumentos insuficientes")
        print("Usage: engine.py <ip_kafka kafak> "
              "<puerto kafka> "
              "<numero maximo de visitantes> "
              "<ip_kafka servidor tiempos> "
              "<puerto ""servidor tiempos>")
        exit(-1)
    """
    # TODO Controlar los datos de entrada para dar error antes de meterlos
    # engine = Engine(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6])
    print_hi('PyCharm')
    VISITANTES = []  # lista con los alias de todos los visitates de los que lleva la cuenta
    MAPA = cargaMapa()
    hilos = [
        LectorMovimientos(argv[1].split(':')[0],
                          int(argv[1].split(':')[1])),
        PideTiempos(argv[3].split(':')[0],
                    int(argv[3].split(':')[1]))
    ]

    TIEMPOS = ""
    for i in hilos:
        i.start()

    time.sleep(10000)

    for i in hilos:
        i.stop()

    for i in hilos:
        i.join()
