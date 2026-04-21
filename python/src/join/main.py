import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

MOM_HOST = os.environ["MOM_HOST"]
INPUT_QUEUE = os.environ["INPUT_QUEUE"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class JoinFilter:
    """
    Recibe un top parcial por cada aggregator por cada query. Como las
    particiones son disjuntas (hash(fruit) % AGGREGATION_AMOUNT), la unión
    es directa: concatenar y quedarse con los TOP_SIZE mayores.

    Mantiene estado por query_id y emite el top final cuando recibió
    AGGREGATION_AMOUNT tops parciales de esa query.
    """

    def __init__(self):
        self.input_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, INPUT_QUEUE
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # { query_id: { "items": [FruitItem...], "partial_count": int } }
        self.states = {}

    def _get_state(self, query_id):
        if query_id not in self.states:
            self.states[query_id] = {"items": [], "partial_count": 0}
        return self.states[query_id]

    def _process_partial_top(self, query_id, partial):
        state = self._get_state(query_id)
        for fruit, amount in partial:
            state["items"].append(fruit_item.FruitItem(fruit, int(amount)))
        state["partial_count"] += 1
        logging.info(
            f"Query {query_id}: partial {state['partial_count']}/{AGGREGATION_AMOUNT}"
        )

        if state["partial_count"] < AGGREGATION_AMOUNT:
            return

        # Merge final: como las particiones son disjuntas, basta con ordenar.
        state["items"].sort()
        state["items"].reverse()
        final_top = state["items"][:TOP_SIZE]
        fruit_top = [[fi.fruit, fi.amount] for fi in final_top]

        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.build_top(query_id, fruit_top)
            )
        )
        logging.info(f"Query {query_id}: final top sent")
        del self.states[query_id]

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            mtype = message_protocol.internal.msg_type(fields)
            query_id = message_protocol.internal.msg_query_id(fields)

            if mtype == message_protocol.internal.MSG_TOP:
                partial = fields[2]
                self._process_partial_top(query_id, partial)
            else:
                logging.warning(f"Unknown msg type in join: {mtype}")

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
        try:
            self.output_queue.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO)
    join = JoinFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping join")
        join.stop()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        join.start()
    finally:
        join.close()
    return 0


if __name__ == "__main__":
    main()