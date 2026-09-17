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

Para facilitar este manejo se agrupan las excepciones de Pika relacionadas con desconexiones en una única tupla. De esta manera, cuando ocurre cualquiera de ellas puede tratarse de forma uniforme y convertirse en la excepción correspondiente del middleware.

Esto permite mantener el encapsulamiento: el código que utiliza el middleware no necesita conocer qué librería se utiliza internamente ni qué excepciones específicas lanza Pika.

## Clase base

`MiddlewareExchange` y `MiddlewareQueue` comparten gran parte de su comportamiento:

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
lambda ch, method, properties, body:
    self._on_message(
        ch,
        method,
        properties,
        body,
        on_message_callback
    )
```

De esta manera, Pika continúa utilizando el formato de callback que espera, mientras que el resto de la aplicación recibe una interfaz propia del middleware y no necesita conocer los detalles de Pika.

## Cola exclusiva del Exchange

Para el middleware basado en `Exchange` se crea una cola sin especificar su nombre:

```python
queue_declare(queue='', exclusive=True)
```

Al utilizar `queue=''`, RabbitMQ genera automáticamente un nombre único para la cola.

Además, `exclusive=True` hace que la cola pertenezca a la conexión que la creó y que sea eliminada cuando dicha conexión se cierre.

Esto permite que cada instancia del consumidor tenga una cola propia y temporal, evitando dejar colas que ya no son utilizadas en RabbitMQ.
