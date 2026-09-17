import pika
from pika.exceptions import (
    AMQPConnectionError,
    ConnectionClosed,
    ChannelClosed,
    StreamLostError,
)

from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareMessageError,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareCloseError,
)

# Se declara a la tupla con un guion bajo ya que es interna del modulo
_DISCONNECTED_ERRORS = (AMQPConnectionError, ConnectionClosed, StreamLostError, ChannelClosed)

# Clase de uso interno con metodos y atributos de uso interno (por eso los _)
class _MessageMiddlewareRabbitMQBase:
    def __init__(self, host):
        try:
            # Abrimos una conexion TCP con RabbitMQ que hace el handshake del protocolo AMQP
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))

            # El canal por el que se van a publicar o consumir los mensajes
            self._channel = self._connection.channel()

        # Manejo de errores de conexion
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        # Manejo de errores genericos inesperados
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        pass

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
