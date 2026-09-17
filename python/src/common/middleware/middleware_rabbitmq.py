import threading

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
        self._lock = threading.Lock()
        self._consuming = False
        self._consumer_tag = None
        self._connection = None
        self._channel = None
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

    def _on_message(self, channel, method, properties, body, on_message_callback):
        def ack():
            # El usuario confirma que proceso el mensaje
            with self._lock:
                # Si hay un channel y esta abierto mando el ack con el tag
                if self._channel is not None and self._channel.is_open:
                    channel.basic_ack(delivery_tag=method.delivery_tag)

        def nack():
            # El usuario no pudo procesar el mensaje
            with self._lock:
                if self._channel is not None and self._channel.is_open:
                    # Si hay un channel y esta abierto mando el nack con el tag y le digo
                    # que intente reenviar el mensaje
                    channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        
        # Lo que exige la interfaz abstracta
        on_message_callback(body, ack, nack)

    def _consume(self, queue_name, on_message_callback):
        # Empiezo a escuchar en una
        try:
            with self._lock:
                # Que RabbitMQ no mande msj nuevo hasta que le llegue ack o nack
                self._channel.basic_qos(prefetch_count=1)

                # Registramos nuestro callback al cual va a llamar Pika en cada msj
                self._consumer_tag = self._channel.basic_consume(
                    queue=queue_name,
                    on_message_callback=lambda ch, method, properties, body: self._on_message(
                        ch, method, properties, body, on_message_callback
                    ),
                )
                self._consuming = True

            # Funcion bloqueante, loop infinito donde procesa msjs
            self._channel.start_consuming()
            self._consuming = False

        except _DISCONNECTED_ERRORS as e:
            self._consuming = False
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except MessageMiddlewareMessageError:
            # Puede pasar que ya este envuelto el error (on_message_callback)
            self._consuming = False
            raise
        except Exception as e:
            self._consuming = False
            raise MessageMiddlewareMessageError(str(e)) from e

    
    def stop_consuming(self):
        try:
            with self._lock:
                # Si el channel existe, estabamos consumiendo y estaba abierto, dejamos de consumir
                if self._consuming and self._channel is not None and self._channel.is_open:
                    self._channel.stop_consuming()
                self._consuming = False
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e

    def close(self):
        try:
            with self._lock:
                if self._channel is not None and self._channel.is_open:
                    self._channel.close()
                if self._connection is not None and self._connection.is_open:
                    self._connection.close()
        except Exception as e:
            # La interfaz pide que un error al cerrar sea CloseError
            raise MessageMiddlewareCloseError(str(e)) from e



class MessageMiddlewareQueueRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareQueue):
 
    def __init__(self, host, queue_name):
        # Conecta y crea el channel
        super().__init__(host)
        self.queue_name = queue_name

        try:
            # Lo que hace queue_declare es, si la cola no existe la crea y 
            # si existe no hace nada. Tambien usamos durable=True que hace
            # que aguante la cola ante reinicios de Rabbit
            self._channel.queue_declare(queue=queue_name, durable=True)
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
 
    def start_consuming(self, on_message_callback):
        # Usamos la logica de consumo de la clase base
        self._consume(self.queue_name, on_message_callback)
 
    def send(self, message):
        try:
            with self._lock:
                # El exchange es el predeterminado que enruta a la queue
                # que tenga de nombre el routing_key. COn delivery_mode=2
                # el mensaje persiste ante caida del broker

                self._channel.basic_publish(
                    exchange='',
                    routing_key=self.queue_name,
                    body=message,
                    properties=pika.BasicProperties(delivery_mode=2),
                )
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
 
 
class MessageMiddlewareExchangeRabbitMQ(_MessageMiddlewareRabbitMQBase, MessageMiddlewareExchange):
 
    def __init__(self, host, exchange_name, routing_keys):
        super().__init__(host)
        self.exchange_name = exchange_name
        # Copiamos la lista
        self.routing_keys = list(routing_keys) if routing_keys else []
        try:

            # Que el exchange sea de tipo direct implica que un mensaje con x
            # routing_key va a todas las colas bindeadas con esa misma key 
            # (broadcast y mensajes puntuales)
            self._channel.exchange_declare(exchange=exchange_name, exchange_type='direct', durable=True)

            # Con queue='' Rabbit genera un nombre unico y exclusive=True 
            # implica que la cola es exclusiva de esta conexion
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue

            # Bindeamos la cola privada a cada routing_key, si se usa como
            # productor no cambia nada
            for routing_key in self.routing_keys:
                self._channel.queue_bind(exchange=exchange_name, queue=self._queue_name, routing_key=routing_key)
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
 
    def start_consuming(self, on_message_callback):
        self._consume(self._queue_name, on_message_callback)
 
    def send(self, message):
        try:
            with self._lock:
                
                # Un mensaje por routing_key
                for routing_key in self.routing_keys:
                    self._channel.basic_publish(
                        exchange=self.exchange_name,
                        routing_key=routing_key,
                        body=message,
                        properties=pika.BasicProperties(delivery_mode=2),
                    )
        except _DISCONNECTED_ERRORS as e:
            raise MessageMiddlewareDisconnectedError(str(e)) from e
        except Exception as e:
            raise MessageMiddlewareMessageError(str(e)) from e
 