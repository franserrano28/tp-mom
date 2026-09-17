# TP-MOM

## Middleware de mensajes

El objetivo del middleware es proporcionar una abstracción sobre RabbitMQ, de manera que el resto del programa no tenga que conocer ni depender directamente de RabbitMQ ni de Pika.

Por lo tanto, dos objetivos principales del middleware son:

* Abstraer el uso de RabbitMQ.
* Encapsular los errores propios de Pika y exponer las excepciones definidas por el contrato del middleware.

## Pika y RabbitMQ

Pika es un cliente de Python para el protocolo AMQP, que es el protocolo utilizado por RabbitMQ para la comunicación entre productores, consumidores y el broker.

En nuestro caso, Pika permite establecer una conexión con RabbitMQ y trabajar mediante un canal (`channel`) para realizar operaciones como publicar y consumir mensajes.

Una conexión representa la conexión con el broker, mientras que un canal es una comunicación lógica dentro de esa conexión que permite realizar las operaciones de AMQP.

## Manejo de excepciones

Pika puede lanzar diferentes excepciones relacionadas con la conexión y los canales. Entre las utilizadas en este trabajo se encuentran:

* `AMQPConnectionError`: errores relacionados con la conexión AMQP.
* `ConnectionClosed`: la conexión fue cerrada.
* `ChannelClosed`: el canal fue cerrado.
* `StreamLostError`: se perdió el stream de comunicación con RabbitMQ.

El contrato definido en `middleware.py` establece las excepciones que debe exponer nuestro middleware. Por este motivo, las excepciones propias de Pika no deben propagarse directamente hacia el resto de la aplicación.

Para facilitar este manejo se agrupan las excepciones de Pika relacionadas con desconexiones en una única tupla:

```python
_DISCONNECTED_ERRORS = (
    AMQPConnectionError,
    ConnectionClosed,
    StreamLostError,
    ChannelClosed,
)
```

De esta manera, cuando ocurre cualquiera de ellas puede tratarse de forma uniforme y convertirse en la excepción correspondiente del middleware.

Esto permite mantener el encapsulamiento: el código que utiliza el middleware no necesita conocer qué librería se utiliza internamente ni qué excepciones específicas lanza Pika.

## Clase base

`MessageMiddlewareExchangeRabbitMQ` y `MessageMiddlewareQueueRabbitMQ` comparten gran parte de su comportamiento:

* conexión con RabbitMQ
* creación y uso del canal
* sincronización mediante un `Lock`
* consumo de mensajes
* detención del consumo
* cierre de la conexión
* manejo y traducción de errores

Implementar esta lógica por separado produciría código duplicado. Por este motivo se creó una tercera clase base interna al módulo, `_MessageMiddlewareRabbitMQBase`, que contiene la funcionalidad común.

Las clases concretas heredan de esta clase y se encargan únicamente de las diferencias propias de cada tipo de middleware.

## Locks

El canal de Pika es utilizado desde distintas operaciones que pueden ejecutarse concurrentemente. Por ejemplo, mientras un thread está procesando un mensaje, puede realizar un `ack` o `nack`, mientras otro thread podría intentar publicar otro mensaje.

Por este motivo se utiliza un `Lock` para proteger las operaciones sobre el canal y evitar que dos threads accedan simultáneamente a una misma operación del canal.

El recurso compartido que protegemos es principalmente el objeto `channel` de Pika, no la cola de RabbitMQ en sí.

El lock se utiliza únicamente durante las operaciones puntuales que acceden al canal y no durante todo el proceso de consumo, ya que `start_consuming()` es una operación bloqueante.

## Exchange y Queue

Un `exchange` y una `queue` cumplen funciones diferentes.

El `exchange` recibe los mensajes publicados y decide a qué colas deben enviarse según su configuración y la `routing_key`.

La `queue` es la estructura donde los mensajes quedan almacenados hasta que un consumidor los recibe.

El flujo puede representarse como:

```text
Producer
   |
   v
Exchange
   |
   | routing key
   v
Queue
   |
   v
Consumer
```

En nuestro caso se utiliza un `direct exchange`, que permite realizar el enrutamiento utilizando una coincidencia exacta entre la `routing_key` del mensaje y la `routing_key` utilizada en el binding de la cola.

## Publicación en Queue

En `MessageMiddlewareQueueRabbitMQ`, los mensajes se publican utilizando el exchange por defecto de RabbitMQ:

```python
exchange=''
```

y como `routing_key` se utiliza el nombre de la cola.

Esto permite publicar directamente en la cola sin utilizar un exchange declarado explícitamente.

## Publicación en Exchange

En `MessageMiddlewareExchangeRabbitMQ`, el middleware recibe una lista de `routing_keys`.

Al enviar un mensaje, se publica utilizando cada una de las claves configuradas:

```python
for routing_key in self.routing_keys:
    self._channel.basic_publish(...)
```

De esta manera, el mensaje se publica en el exchange utilizando cada `routing_key`, permitiendo que sea enviado a las colas que estén vinculadas al exchange mediante esas claves de enrutamiento.

## Confirmación de procesamiento

El middleware proporciona al callback dos funciones: `ack` y `nack`.

`ack` indica a RabbitMQ que el mensaje fue procesado correctamente y que puede ser eliminado de la cola.

`nack` indica que el procesamiento falló. En este caso se utiliza `requeue=True`, por lo que RabbitMQ vuelve a colocar el mensaje en la cola para que pueda ser procesado nuevamente.

Esto permite que la decisión sobre el resultado del procesamiento quede en manos del consumidor, sin exponer directamente las operaciones de Pika al resto de la aplicación.

Cada mensaje entregado por RabbitMQ posee un `delivery_tag`, que permite identificar específicamente qué entrega se está confirmando o rechazando.

## Control de mensajes pendientes

Al iniciar el consumo se configura:

```python
self._channel.basic_qos(prefetch_count=1)
```

Esto limita la cantidad de mensajes que RabbitMQ puede entregar al consumidor sin recibir previamente su `ack`.

De esta manera, un consumidor no recibe varios mensajes pendientes de procesamiento al mismo tiempo y se favorece una distribución más equilibrada del trabajo entre consumidores.

## Callback

Pika espera que el callback utilizado para consumir mensajes reciba los argumentos correspondientes a la entrega:

```python
callback(channel, method, properties, body)
```

Por lo tanto, el callback que se registra directamente en Pika debe aceptar esos argumentos.

Sin embargo, el middleware define una interfaz diferente para el callback que recibe el usuario. En lugar de exponer detalles de Pika, se le entrega:

```python
on_message_callback(body, ack, nack)
```

Para realizar esta adaptación se utiliza un `lambda` intermedio:

```python
lambda ch, method, properties, body: self._on_message(
    ch, method, properties, body, on_message_callback
)
```

Pika continúa utilizando el formato de callback que espera, mientras que el resto de la aplicación recibe una interfaz propia del middleware y no necesita conocer los detalles de Pika.

## Cola exclusiva del Exchange

Para el middleware basado en `Exchange` se crea una cola sin especificar su nombre:

```python
queue_declare(queue='', exclusive=True)
```

Al utilizar `queue=''`, RabbitMQ genera automáticamente un nombre único para la cola.

Además, `exclusive=True` hace que la cola pertenezca a la conexión que la creó y que sea eliminada cuando dicha conexión se cierre.

Esto permite que cada instancia del consumidor tenga una cola propia y temporal, evitando dejar colas que ya no son utilizadas en RabbitMQ.

Luego, la cola se vincula al exchange mediante cada una de las `routing_keys` configuradas.

## Persistencia

Las colas y exchanges utilizados por el middleware se declaran como `durable=True`, de modo que sus definiciones sobrevivan a un reinicio del broker.

Además, los mensajes publicados utilizan:

```python
pika.BasicProperties(delivery_mode=2)
```

indicando que son mensajes persistentes.

Estas configuraciones permiten mantener la definición de las colas y exchanges y la persistencia de los mensajes ante un reinicio de RabbitMQ, siempre que el broker esté configurado para persistirlos correctamente.

## Ciclo de vida del consumidor

`start_consuming` inicia el consumo de mensajes mediante `start_consuming()` de Pika.

`stop_consuming` permite detener el consumo sin cerrar necesariamente la conexión con RabbitMQ.

Por otro lado, `close` se utiliza para liberar los recursos asociados al middleware. Primero se cierra el canal y luego la conexión con RabbitMQ.
