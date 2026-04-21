import json

# Tipos de mensaje del protocolo interno.
# Cada mensaje es una lista JSON cuyo primer elemento es el tipo.
# Se usa el primer elemento como discriminador en vez de len(fields) para
# que sea explícito y extensible.
MSG_DATA = "D"    # ["D", query_id, fruit, amount]
MSG_EOF = "E"     # ["E", query_id, source]
MSG_TOP = "T"     # ["T", query_id, [[fruit, amount], ...]]

# Subtipo de EOF: indica quién originó el mensaje.
# - EOF_SOURCE_HANDLER: lo emitió el MessageHandler del gateway cuando el
#   cliente terminó de enviar datos. Se publica UNA sola vez al input_queue.
# - EOF_SOURCE_SUM: lo emite un sum para avisar a los demás sums que el
#   cliente terminó. Se publican SUM_AMOUNT copias (una por sum).
# Esta distinción evita que los sums repliquen EOFs indefinidamente.
EOF_SOURCE_HANDLER = "H"
EOF_SOURCE_SUM = "S"


def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    return json.loads(message.decode("utf-8"))


def build_data(query_id, fruit, amount):
    return [MSG_DATA, query_id, fruit, amount]


def build_eof(query_id, source):
    """source: EOF_SOURCE_HANDLER o EOF_SOURCE_SUM"""
    return [MSG_EOF, query_id, source]


def build_top(query_id, fruit_top):
    """fruit_top: lista de [fruit, amount]"""
    return [MSG_TOP, query_id, fruit_top]


def msg_type(fields):
    return fields[0]


def msg_query_id(fields):
    return fields[1]


def eof_source(fields):
    """Devuelve el origen de un mensaje EOF (EOF_SOURCE_HANDLER o EOF_SOURCE_SUM)."""
    return fields[2]