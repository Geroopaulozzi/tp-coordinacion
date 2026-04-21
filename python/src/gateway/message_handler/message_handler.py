import uuid

from common import message_protocol


class MessageHandler:
    """
    Hay una instancia por cliente conectado. En __init__ se genera un
    query_id único que acompaña a cada mensaje interno emitido por este
    cliente. Los filtros (sum, aggregation, join) mantienen estado por
    query_id, lo que permite atender múltiples clientes concurrentemente
    sin mezclar sus datos.

    El gateway itera sobre la lista de MessageHandler activos al recibir
    un top desde el pipeline; cada handler decide si el mensaje es suyo
    comparando el query_id embebido con el propio. El handler dueño
    devuelve el top (truthy) y los demás None (falsy), respetando el
    contrato del gateway original sin necesidad de tocarlo.
    """

    def __init__(self):
        self.query_id = uuid.uuid4().hex

    def serialize_data_message(self, message):
        """message: (fruit, amount) tal como lo entrega el recv externo."""
        fruit, amount = message
        return message_protocol.internal.serialize(
            message_protocol.internal.build_data(self.query_id, fruit, amount)
        )

    def serialize_eof_message(self, message):
        """
        Emite un único EOF marcado como EOF_SOURCE_HANDLER. El gateway no
        necesita conocer SUM_AMOUNT: es el primer sum que recibe este EOF
        el que replica SUM_AMOUNT copias marcadas como EOF_SOURCE_SUM al
        mismo input_queue. Como la cola es FIFO, los EOF replicados quedan
        detrás de todos los DATA pendientes, garantizando que cuando cada
        sum los vea ya habrá procesado su fracción de datos de esta query.
        """
        return message_protocol.internal.serialize(
            message_protocol.internal.build_eof(
                self.query_id, message_protocol.internal.EOF_SOURCE_HANDLER
            )
        )

    def deserialize_result_message(self, message):
        """
        Devuelve el fruit_top si el mensaje corresponde a este cliente,
        o None en caso contrario. El gateway usa el valor devuelto como
        flag de pertenencia (truthy = mío, falsy = de otro).
        """
        fields = message_protocol.internal.deserialize(message)
        if message_protocol.internal.msg_type(fields) != message_protocol.internal.MSG_TOP:
            return None
        if message_protocol.internal.msg_query_id(fields) != self.query_id:
            return None
        return fields[2]  # lista de [fruit, amount]