import pika
import signal
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)

# Cantidad máxima de mensajes no confirmados entregados a cada consumidor a la vez.
# Valor 1 garantiza reparto equitativo entre consumidores concurrentes.
PREFETCH_COUNT = 1


def _build_pika_callback(on_message_callback):
    """
    Wrappea el callback de pika para exponer la interfaz (message, ack, nack)
    definida por la clase abstracta, ocultando los detalles de pika al consumidor.
    El ack y nack son invocados manualmente por el consumidor, lo que garantiza
    que un mensaje solo se confirma tras ser procesado exitosamente (at-least-once).
    """
    def _callback(ch, method, properties, body):
        ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)
        on_message_callback(body, ack, nack)
    return _callback


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._channel = None
        self._connection = None
        self._consumer_tag = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
            # durable=True: la cola sobrevive reinicios del broker.
            self._channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ: {e}")

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        self.close()

    def start_consuming(self, on_message_callback):
        try:
            # prefetch_count=1 evita que un consumidor acumule mensajes.
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_pika_callback(on_message_callback),
                auto_ack=False
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")
        finally:
            # Se limpia el tag independientemente de cómo terminó el loop, para que stop_consuming no intente detener un consumer inexistente.
            self._consumer_tag = None

    def stop_consuming(self):
        """
        Detiene el loop de consumo si esta instancia tenía un consumer activo.
        Si no se estaba consumiendo, no tiene efecto.
        """
        try:
            if self._channel and self._channel.is_open and self._consumer_tag:
                self._channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stopping: {e}")

    def send(self, message):
        try:
            # exchange='' usa el exchange defult de RabbitMQ que rutea directamente a la cola cuyo nombre coincide con la routing_key
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message,
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Conection lost while sending: {e}")
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

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._channel = None
        self._connection = None
        self._queue_name = None
        self._consumer_tag = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type='direct',
                durable=True
            )
            # Cola exclusiva y anónima por instancia: RabbitMQ genera un nombre único y la elimina al cerrar la conexión. Necesaria para broadcast:
            # cada consumer tiene su propia cola bindeada a la routing key, por lo que recibe su propia copia de cada mensaje.
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue
            for routing_key in routing_keys:
                self._channel.queue_bind(
                    exchange=exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Filed to connect to RabitMQ: {e}")

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        self.close()

    def start_consuming(self, on_message_callback):
        try:
            self._channel.basic_qos(prefetch_count=PREFETCH_COUNT)
            self._consumer_tag = self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_pika_callback(on_message_callback),
                auto_ack=False
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming: {e}")
        except pika.exceptions.AMQPError as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")
        finally:
            self._consumer_tag = None

    def stop_consuming(self):
        """
        Detiene el loop de consumo si esta instancia tenía un consumer activo.
        Si no se estaba consumiendo, no tiene efecto.
        """
        try:
            if self._channel and self._channel.is_open and self._consumer_tag:
                self._channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stopping: {e}")

    def send(self, message):
        # Según el criterio de la cátedra, send publica a todas las routing keys configuradas en esta instanciq
        try:
            for routing_key in self._routing_keys:
                self._channel.basic_publish(
                    exchange=self._exchange_name,
                    routing_key=routing_key,
                    body=message,
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending: {e}")
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