# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
"""
    Jorge Sanchez Pastor 49779447N
"""
import random
import signal
import time
import traceback
from sys import argv
import threading
import socket
import kafka
from kafka import KafkaConsumer, KafkaProducer, KafkaAdminClient
import sqlite3

from kafka.admin import NewTopic


class LectorMovimientos(threading.Thread):
    def __init__(self, ip, port, database):
        self.database = database
        self.ip_kafka = ip
        self.port_kakfa = port
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.visitantes = {}
        self.posiciones = {}

    def stop(self):
        self.stop_event.set()

    def recuperaPosiciones(self):
        con = sqlite3.connect(self.database)
        cur = con.cursor()
        for i in cur.execute("select * from atracciones"):
            self.posiciones[i[0].encode()] = int(i[1])
        con.close()
        print("ATRACCIONES RECUPERADAS", self.posiciones)

    def actualizaMapa(self):
        global TIEMPOS
        global MAPA
        global VISITANTES

        mapa = [-1 for i in range(0, 400)]
        visitantes = VISITANTES
        #print("v: ",visitantes)
        tiempos = TIEMPOS
        # print("t:", tiempos)
        # print("l:", self.posiciones)
        # print("v:", visitantes)

        for j in visitantes:
            if visitantes[j] != b'no':
                mapa[int(visitantes[j])] = -(list(visitantes.keys()).index(j)) - 2

        for j in self.posiciones:
            if j in tiempos:
                mapa[self.posiciones[j]] = tiempos[j]
        MAPA = mapa

    def run(self):
        print("INICIO LectorMovimientos")
        self.recuperaPosiciones()
        try:
            while (not self.stop_event.is_set()) and running:
                self.actualizaMapa()
                time.sleep(0.1)
        except Exception as e:
            print("ERROR EN LectorMovimientos :", e)
            traceback.print_exc()
        finally:
            print("FIN LectorMovimientos")


def actualizaTiempos(tiempos: dict):
    """
    Actualiza los timepos en el mapa
    """
    global TIEMPOS
    TIEMPOS = tiempos
    # print(f"TT: {TIEMPOS}")


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

    def run(self):
        HEADER = 10

        while (not self.stop_event.is_set()) and running:
            try:
                cliente = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                # print(self.ip, self.port)
                cliente.connect((self.ip, self.port))
                # print("conectado 1")
                size = int(cliente.recv(HEADER))
                # print("size: ", size)
                tiempos = cliente.recv(size)
                # print(f"he recibido {tiempos}")
                tiempos = eval(tiempos.decode())
                # print("evaluado")
                actualizaTiempos(tiempos)
                cliente.close()
            except Exception as e:
                print("ERROR PideTiempos:", e)
            finally:
                if 'cliente' in locals():
                    cliente.close()
                time.sleep(3)


class AccesManager(threading.Thread):
    """
    Clase Thread que se conecta cada 3 segundos al servidor de tiempos que le
    hayamos indicado.
    """

    def __init__(self, ip, port, database):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port
        self.database = database

    def stop(self):
        self.stop_event.set()

    def login(self, alias, passwd):
        final = False
        con = sqlite3.connect(self.database)
        cur = con.cursor()
        sql_comand = f"select * from users where " \
                     f"alias like '{alias}' and " \
                     f"passwd like '{passwd}';"
        print("haciendo login: ", sql_comand)
        try:
            cur.execute(sql_comand)
            for _ in cur.execute(sql_comand):
                final = True
                print(f"LOGIN CON EXITO ")
            con.commit()
        except Exception as e:
            print("ERROR al registrar", e)
            final = False
        finally:
            con.close()
            return final

    def createTopic(self, topic):
        consumer = kafka.KafkaConsumer(group_id='test', bootstrap_servers=f'{self.ip}:{self.port}')
        tops = consumer.topics()
        if (topic + 'in' not in tops) and (topic + 'out' not in tops):
            admin = KafkaAdminClient(bootstrap_servers=f'{self.ip}:{self.port}')
            topic_list = [NewTopic(name=topic + 'in', num_partitions=2, replication_factor=1)]
            topic_list += [NewTopic(name=topic + 'out', num_partitions=2, replication_factor=1)]
            admin.create_topics(new_topics=topic_list, validate_only=False)

    def consumir(self, consumer, producer):
        global LIMITE
        manejadores = []
        while (not self.stop_event.is_set()) and running:
            for msg in consumer:
                if len(manejadores) >= LIMITE:
                    producer.send("accesoout", b'no')  # todo poner otro codgio para cunado hay demansiados
                try:
                    alias = msg.value.decode().split(".")[0]
                    passwd = msg.value.decode().split(".")[1]
                    if self.login(alias, passwd) and len(manejadores) < LIMITE:
                        print(f"Login Exito {alias} {passwd}")
                        topic = alias+str(int(time.time()))
                        self.createTopic(topic)
                        manejadores.append(AtieneVisitante(self.ip, self.port, topic))
                        manejadores[-1].start()
                        time.sleep(1)
                        producer.send("accesoout", topic.encode())
                    else:
                        time.sleep(1)
                        print(f"Login Fallo {alias} {passwd}")
                        producer.send("accesoout", b'no')
                except Exception as e:
                    print("ERROR EN AccesManager consumir", e)
                    traceback.print_exc()

    def run(self):
        print("INICIO LectorMovimientos")
        try:
            consumer = KafkaConsumer(bootstrap_servers=f'{self.ip}:{self.port}',
                                     auto_offset_reset='latest',
                                     consumer_timeout_ms=100)
            consumer.subscribe(['accesoin'])

            producer = KafkaProducer(bootstrap_servers=f'{self.ip}:{self.port}')
            self.consumir(consumer, producer)
        except Exception as e:
            print("ERROR EN LectorMovimientos :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            print("FIN LectorMovimientos")


class AtieneVisitante(threading.Thread):
    """
    Clase Thread que se conecta cada 3 segundos al servidor de tiempos que le
    hayamos indicado.
    """

    def __init__(self, ip, port, topic):
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()
        self.ip = ip
        self.port = port
        self.topic = topic

    def stop(self):
        self.stop_event.set()

    def consumir(self, consumer, producer):
        global TIEMPOS
        global MAPA
        while (not self.stop_event.is_set()) and running:
            """
            if time.time() - anterior > 5:
                print(f"AtieneVisitante envio de mapa para {self.topic} por despecho")
                producer.send(self.topic + 'out', str(MAPA).encode())
            """
            for msg in consumer:
                print(self.topic, " : ", msg.value)
                try:
                    if msg.value == b'no' or not running:
                        del (VISITANTES[self.topic])
                        print("SALE EL VISITANTE: ",self.topic)
                        self.stop_event.set()
                        self.stop()
                        return
                    VISITANTES[self.topic] = msg.value
                    print(f"AtieneVisitante envio de mapa para {self.topic}")
                    producer.send(self.topic + 'out', str(MAPA).encode())
                except Exception as e:
                    print("ERROR EN AtieneVisitante consumir", e)
                    traceback.print_exc()

    def run(self):
        print("INICIO AtieneVisitante " + self.topic)
        try:
            consumer = KafkaConsumer(bootstrap_servers=f'{self.ip}:{self.port}',
                                     auto_offset_reset='latest',
                                     consumer_timeout_ms=100)
            consumer.subscribe([self.topic + 'in'])
            producer = KafkaProducer(bootstrap_servers=f'{self.ip}:{self.port}')
            self.consumir(consumer, producer)
        except Exception as e:
            print("ERROR EN LectorMovimientos :", e)
            traceback.print_exc()
        finally:
            if 'consumer' in locals():
                consumer.close()
            if 'producer' in locals():
                producer.close()
            print("FIN LectorMovimientos")


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
    global running
    running = False
    print("Holas")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    """
    if len(argv) < 6:
        print("Argumentos insuficientes")
        print("Usage: FWQ_Engine.py <ip_kafka kafak> "
              "<puerto kafka> "
              "<numero maximo de visitantes> "
              "<ip_kafka servidor tiempos> "
              "<puerto ""servidor tiempos>")
        exit(-1)
    """
    #
    # engine = Engine(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6])
    # GLOBALES
    signal.signal(signal.SIGINT, manejador)
    running = True
    VISITANTES = {}  # lista con los alias de todos los visitates de los que lleva la cuenta
    TIEMPOS = {}
    MAPA = [-1 for i in range(0, 400)]
    LIMITE = int(argv[2])
    print(argv)
    hilos = [
        LectorMovimientos(argv[1].split(':')[0],
                          int(argv[1].split(':')[1]),
                          argv[4]),
        AccesManager(argv[1].split(':')[0],
                     int(argv[1].split(':')[1]),
                     argv[4]),
        PideTiempos(argv[3].split(':')[0],
                    int(argv[3].split(':')[1]))
    ]

    for i in hilos:
        i.start()

    while running:
        pass
    time.sleep(1)

    for i in hilos:
        i.stop()

    for i in hilos:
        i.join()
