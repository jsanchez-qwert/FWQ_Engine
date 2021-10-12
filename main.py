# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
"""
    TODO PREGUNTAR AL PROFESOR POR EL LOGIN
"""
from sys import argv


class Engine:
    def __init__(self, ip_k: str, port_k: int, max_visitantes: int, ip_w: str, port_w: int):
        self.ip_k = ip_k
        self.port_k = port_k
        self.max_visitantes = max_visitantes
        self.ip_w = ip_w
        self.port_w = port_w


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


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    if len(argv) < 6:
        print("Argumentos insuficientes")
        print("Usage: engine.py <ip kafak> "
              "<puerto kafka> "
              "<numero maximo de visitantes> "
              "<ip servidor tiempos> "
              "<puerto ""servidor tiempos>")
        exit(-1)
        # TODO Controlar los datos de entrada para dar error antes de meterlos
    engine = Engine(argv[1], argv[2], argv[3], argv[4], argv[5], argv[6])
    print_hi('PyCharm')

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
