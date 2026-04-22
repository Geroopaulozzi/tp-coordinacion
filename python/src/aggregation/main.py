import os
import logging
import signal

from common import middleware, message_protocol, fruit_item

ID = int(os.environ["ID"])
MOM_HOST = os.environ["MOM_HOST"]
OUTPUT_QUEUE = os.environ["OUTPUT_QUEUE"]
SUM_AMOUNT = int(os.environ["SUM_AMOUNT"])
SUM_PREFIX = os.environ["SUM_PREFIX"]
AGGREGATION_AMOUNT = int(os.environ["AGGREGATION_AMOUNT"])
AGGREGATION_PREFIX = os.environ["AGGREGATION_PREFIX"]
TOP_SIZE = int(os.environ["TOP_SIZE"])


class AggregationFilter:
    """
    Consume datos de su particionn (routing key agregation_<ID>) provenientes
    de todos los sums. Mantiene un acumulado por query_id. Cuando recibe
    SUM_AMOUNT EOFs de una query (uno por cada sum), calcula el top parcial
    de esa partición y lo envía al join.
    """

    def __init__(self):
        self.input_exchange = middleware.MessageMiddlewareExchangeRabbitMQ(
            MOM_HOST, AGGREGATION_PREFIX, [f"{AGGREGATION_PREFIX}_{ID}"]
        )
        self.output_queue = middleware.MessageMiddlewareQueueRabbitMQ(
            MOM_HOST, OUTPUT_QUEUE
        )
        # { query_id: { "items": {fruit: FruitItem}, "eof_count": int } }
        self.states = {}

    def _get_state(self, query_id):
        if query_id not in self.states:
            self.states[query_id] = {"items": {}, "eof_count": 0}
        return self.states[query_id]

    def _process_data(self, query_id, fruit, amount):
        state = self._get_state(query_id)
        current = state["items"].get(fruit, fruit_item.FruitItem(fruit, 0))
        state["items"][fruit] = current + fruit_item.FruitItem(fruit, int(amount))

    def _process_eof(self, query_id):
        state = self._get_state(query_id)
        state["eof_count"] += 1
        logging.info(
            f"Query {query_id}: EOF {state['eof_count']}/{SUM_AMOUNT}"
        )

        if state["eof_count"] < SUM_AMOUNT:
            return

        # Todos los sums terminaron su porción para esta query.
        items = sorted(state["items"].values())
        items.reverse()
        top = items[:TOP_SIZE]
        fruit_top = [[fi.fruit, fi.amount] for fi in top]

        self.output_queue.send(
            message_protocol.internal.serialize(
                message_protocol.internal.build_top(query_id, fruit_top)
            )
        )
        logging.info(f"Query {query_id}: partial top sent ({len(fruit_top)} items)")
        del self.states[query_id]

    def process_message(self, message, ack, nack):
        try:
            fields = message_protocol.internal.deserialize(message)
            mtype = message_protocol.internal.msg_type(fields)
            query_id = message_protocol.internal.msg_query_id(fields)

            if mtype == message_protocol.internal.MSG_DATA:
                _, _, fruit, amount = fields
                self._process_data(query_id, fruit, amount)
            elif mtype == message_protocol.internal.MSG_EOF:
                self._process_eof(query_id)
            else:
                logging.warning(f"Unknown msg type: {mtype}")

            ack()
        except Exception as e:
            logging.error(f"Error processing message: {e}")
            nack()

    def start(self):
        self.input_exchange.start_consuming(self.process_message)

    def stop(self):
        self.input_exchange.stop_consuming()

    def close(self):
        try:
            self.input_exchange.close()
        except Exception:
            pass
        try:
            self.output_queue.close()
        except Exception:
            pass


def main():
    logging.basicConfig(level=logging.INFO)
    agg = AggregationFilter()

    def _handle_sigterm(signum, frame):
        logging.info("SIGTERM received, stopping aggregation")
        agg.stop()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    try:
        agg.start()
    finally:
        agg.close()
    return 0


if __name__ == "__main__":
    main()