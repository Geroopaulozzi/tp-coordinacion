import pika
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

# Cantidad máxima de mensajes no confirmados entregados a cada consumer a la vez.
# Valor 1 garantiza reparto equitativo entre consumers concurrentes y evita que
# un consumer acumule trabajo. Es clave para que el token-ring de EOFs en los
# sums funcione correctamente (ver sum/main.py).
PREFETCH_COUNT = 1


def _build_pika_callback(on_message_callback):
    """
    Wrappea el callback de pika para exponer la interfaz (message, ack, nack)
    definida por la clase abstracta, ocultando los detalles de pika al consumer.
    El ack/nack se invocan manualmente: un mensaje solo se confirma tras ser
    procesado exitosamente (at-least-once).
    """
    def _callback(ch, method, properties, body):
        ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        on_message_callback(body, ack, nack)
    return _callback


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):
    """
    Cola compartida: con N consumers subscriptos, RabbitMQ reparte round-robin.
    La cola es durable para sobrevivir reinicios del broker.
    """

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._channel = None
        self._connection = None
        self._consumer_tag = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, heartbeat=600)
            )
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Failed to connect to RabbitMQ: {e}"
            )

    def start_consuming(self, on_message_callback):
        try:
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_pika_callback(on_message_callback),
                auto_ack=False,
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while consuming: {e}"
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")
        finally:
            self._consumer_tag = None

    def stop_consuming(self):
        """
        Thread-safe: puede invocarse desde un signal handler u otro hilo,
        ya que agenda la detención en el event loop de pika.
        """
        if not self._connection or not self._connection.is_open:
            return
        try:
            self._connection.add_callback_threadsafe(self._safe_stop)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while stopping: {e}"
            )

    def _safe_stop(self):
        if self._channel and self._channel.is_open and self._consumer_tag:
            self._channel.stop_consuming()

    def send(self, message):
        try:
            # exchange='' = default exchange: rutea directo a la cola con
            # nombre igual al routing_key.
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while sending: {e}"
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while sending: {e}")

    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error while closing: {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    """
    Exchange direct con routing keys explícitas.
    La cola consumer se crea lazy al llamar start_consuming para evitar que
    una instancia que solo publica cree colas anónimas fantasma que acumulan
    mensajes sin consumer (memory leak en el broker).
    """

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._channel = None
        self._connection = None
        self._queue_name = None  # Se setea solo si se consume
        self._consumer_tag = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host, heartbeat=600)
            )
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type="direct",
                durable=True,
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Failed to connect to RabbitMQ: {e}"
            )

    def _declare_consumer_queue(self):
        """
        Crea una cola exclusiva anónima bindeada a las routing keys. Solo se
        invoca cuando esta instancia va a consumir, no cuando solo publica.
        """
        result = self._channel.queue_declare(queue="", exclusive=True)
        self._queue_name = result.method.queue
        for routing_key in self._routing_keys:
            self._channel.queue_bind(
                exchange=self._exchange_name,
                queue=self._queue_name,
                routing_key=routing_key,
            )

    def start_consuming(self, on_message_callback):
        try:
            if self._queue_name is None:
                self._declare_consumer_queue()
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_pika_callback(on_message_callback),
                auto_ack=False,
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while consuming: {e}"
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")
        finally:
            self._consumer_tag = None

    def stop_consuming(self):
        if not self._connection or not self._connection.is_open:
            return
        try:
            self._connection.add_callback_threadsafe(self._safe_stop)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while stopping: {e}"
            )

    def _safe_stop(self):
        if self._channel and self._channel.is_open and self._consumer_tag:
            self._channel.stop_consuming()

    def send(self, message):
        # Publica a todas las routing keys configuradas en esta instancia.
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(
                f"Connection lost while sending: {e}"
            )
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while sending: {e}")

    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareCloseError(f"Error while closing: {e}")