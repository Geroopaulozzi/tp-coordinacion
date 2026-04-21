import os
import logging
import signal
import hashlib

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]


def _partition_for(fruit, n):
    """
    Determina a qué aggregator le corresponde una fruta. Se usa hash estable
    (MD5) porque el builtin hash() varía entre procesos según PYTHONHASHSEED
    y rompería el particionamiento entre réplicas.
    """
    h = hashlib.md5(fruit.encode("utf-8")).hexdigest()
    return int(h, 16) % n


class SumFilter:
    """
    Acumula sumas parciales por query_id. Como la cola input_queue es
    compartida entre todos los sums (round-robin), cada sum ve solo una
    fracción de los datos de cada cliente. La coordinación del fin-de-stream
    funciona así:

    1) El gateway publica UN solo EOF(source=HANDLER) al input_queue cuando
       el cliente manda END_OF_RECORDS.
    2) El sum que lo recibe publica SUM_AMOUNT copias de EOF(source=SUM) al
       mismo input_queue y descarta el original. Como la cola es FIFO y los
       EOFs se agregan al final, todos los sums ya procesaron sus datos de
       esa query antes de ver ningún EOF_SUM.
    3) Cada sum recibe exactamente un EOF_SUM por query, hace flush de su
       estado local hacia los aggregators (particionando por hash(fruit)) y
       borra el estado. No replica nada más.

    Esta distinción evita loops de replicación y garantiza que el flush
    ocurra después de haber procesado todos los datos de la query.
    """

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        # Una instancia por aggregator: cada una publica a una routing key
        # distinta del exchange compartido.
        self.aggregator_exchanges = []
        for i in range(AGGREGATION_AMOUNT):
            self.aggregator_exchanges.append(
                middleware.MessageMiddlewareExchangeRabbitMQ(
                    MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{i}"]
                )
            )
        # Estado por consulta: { query_id: { fruit: FruitItem } }
        self.states = {}

    def _process_data(self, query_id, fruit, amount):
        state = self.states.setdefault(query_id, {})
        current = state.get(fruit, fruit_item.FruitItem(fruit, 0))
        state[fruit] = current + fruit_item.FruitItem(fruit, int(amount))

    def _flush_to_aggregators(self, query_id):
        """
        Envía los datos de la query particionados por hash a los aggregators
        correspondientes, y un EOF a cada aggregator (aunque no haya datos
        en esa partición). El aggregator cuenta SUM_AMOUNT EOFs para saber
        cuándo todos los sums terminaron.
        """
        state = self.states.pop(query_id, {})
        logging.info(f"Flushing query {query_id}: {len(state)} fruits")

        by_agg = [[] for _ in range(AGGREGATION_AMOUNT)]
        for fi in state.values():
            idx = _partition_for(fi.fruit, AGGREGATION_AMOUNT)
            by_agg[idx].append(fi)

        for idx, items in enumerate(by_agg):
            for fi in items:
                self.aggregator_exchanges[idx].send(
                    message_protocol.internal.serialize(
                        message_protocol.internal.build_data(
                            query_id, fi.fruit, fi.amount
                        )
                    )
                )
            self.aggregator_exchanges[idx].send(
                message_protocol.internal.serialize(
                    message_protocol.internal.build_eof(
                        query_id, message_protocol.internal.EOF_SOURCE_SUM
                    )
                )
            )

    def _replicate_eof_to_sums(self, query_id):
        """
        Publica SUM_AMOUNT copias de EOF_SUM al input_queue. La cola FIFO
        garantiza que estos mensajes queden detrás de cualquier DATA aún
        pendiente, por lo que todos los sums los recibirán recién cuando
        hayan terminado de procesar su fracción de datos de esta query.
        """
        for _ in range(SUM_AMOUNT):
            self.input_queue.send(
                message_protocol.internal.serialize(
                    message_protocol.internal.build_eof(
                        query_id, message_protocol.internal.EOF_SOURCE_SUM
                    )
                )
            )

    def _process_eof(self, query_id, source):
        if source == message_protocol.internal.EOF_SOURCE_HANDLER:
            # Llegó el EOF original del gateway: replico SUM_AMOUNT EOF_SUM
            # y NO hago flush con este mensaje (este sum flusheará cuando le
            # llegue su propio EOF_SUM, garantizando orden con sus datos).
            logging.info(f"Query {query_id}: EOF_HANDLER received, replicating")
            self._replicate_eof_to_sums(query_id)
        elif source == message_protocol.internal.EOF_SOURCE_SUM:
            # Llegó un EOF replicado: todos mis datos de esta query ya están
            # procesados (la cola FIFO lo garantiza). Flush y listo.
            logging.info(f"Query {query_id}: EOF_SUM received, flushing")
            self._flush_to_aggregators(query_id)
        else:
            logging.warning(f"Unknown EOF source: {source}")

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            mtype = message_protocol.internal.msg_type(fields)
            query_id = message_protocol.internal.msg_query_id(fields)

            if mtype == message_protocol.internal.MSG_DATA:
                _, _, fruit, amount = fields
                self._process_data(query_id, fruit, amount)
            elif mtype == message_protocol.internal.MSG_EOF:
                source = message_protocol.internal.eof_source(fields)
                self._process_eof(query_id, source)
            else:
                logging.warning(f"Unknown msg type: {mtype}")

            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def start(self):
        self.input_queue.start_consuming(self.process_message)

    def stop(self):
        self.input_queue.stop_consuming()

    def close(self):
        try:
            self.input_queue.close()
        except Exception:
            pass
        for ex in self.aggregator_exchanges:
            try:
                ex.close()
            except Exception:
                pass


def main():
    logging.basicConfig(level=logging.INFO)
    sum_filter = SumFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping sum")
        sum_filter.stop()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        sum_filter.start()
    finally:
        sum_filter.close()
    return 0


if __name__ == "__main__":
    main()