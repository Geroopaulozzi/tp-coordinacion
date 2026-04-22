# TP Coordinación — Informe de Diseño


El sistema procesa streams concurrentes de clientes que envían pares
`(fruta, cantidad)` al gateway y reciben de vuelta un top-N con las
frutas más acumuladas. El pipeline se compone de cuatro tipos de
controles replicables (Gateway, Sum, Aggregation, Join) comunicados
mediante colas y exchanges de RabbitMQ.

El desafío principal es coordinar las distintas instancias sin estado
compartido para que varios clientes sean atendidos de forma concurrente
sin que sus datos se mezclen, que los filtros replicables dividan el
trabajo en lugar de duplicarlo, y que la detección de fin de stream sea
correcta incluso cuando los datos de un cliente están distribuidos
entre múltiples réplicas.

La solución se apoya en tres ideas: un `query_id` por cliente que viaja
con cada mensaje interno, un protocolo de EOF en dos fases sobre la
misma cola FIFO, y particionamiento por hash desde los Sums hacia los
Aggregators.


## Identificación de consultas

Al conectarse un cliente, el `MessageHandler` asociado genera un
`query_id` único (UUID). Ese identificador acompaña a todos los
mensajes internos (`DATA`, `EOF`, `TOP`) que ese cliente produce en el
pipeline. Cada filtro mantiene su estado indexado por `query_id`, lo
que actúa como separador lógico entre consultas y permite interleave
total: N clientes pueden enviar datos simultáneamente y cada filtro los
procesa a medida que llegan sin coordinación extra.

Cuando el Join emite el top final, lo hace con el `query_id`
correspondiente. El gateway devuelve el resultado al cliente correcto
porque cada `MessageHandler` (instanciado por el gateway al aceptar
cada cliente) filtra los mensajes de la `results_queue` comparando el
`query_id` embebido con el propio, devolviendo truthy cuando el top le
pertenece y `None` en caso contrario. El gateway original ya consume
esa señal, por lo que no hace falta modificar su `main.py`.


## Coordinación de los Sums

Los Sums consumen desde `input_queue`, que es una cola compartida que
RabbitMQ reparte round-robin entre los consumers. Esto implica que los
datos de un mismo cliente se distribuyen entre las N réplicas de Sum:
cada una recibe solo una fracción.

El problema aparece al marcar el fin de stream. Si el gateway publicara
un único `EOF(query_id)` al terminar el cliente, ese EOF podría ser
consumido por cualquier Sum, incluso por uno que todavía no haya
procesado varios DATA de esa query encolados detrás de mensajes de
otros clientes. El Sum haría flush de un estado incompleto y el total
final sería incorrecto. El desafío es entonces garantizar que cuando un
Sum procese el EOF de una query, ya haya leído todos los DATA de esa
query que le correspondían.

La solución se apoya en la garantía FIFO de RabbitMQ dentro de una
misma cola y en distinguir dos subtipos de EOF según su origen. El
`MessageHandler` emite un único `EOF(query_id, "H")` al `input_queue`
cuando recibe el `END_OF_RECORDS` del cliente. El primer Sum que lo
consume, al ver el origen `"H"`, no hace flush: publica `SUM_AMOUNT`
copias de `EOF(query_id, "S")` al mismo `input_queue` y descarta el
original. Eventualmente cada Sum recibe exactamente un `EOF` con origen
`"S"`, y en ese momento sí hace flush del estado de esa query hacia los
Aggregators, liberándolo luego.

Como los EOF replicados se publican al mismo `input_queue` **después**
de que todos los DATA de esa query ya fueron publicados por el gateway,
quedan necesariamente detrás de cualquier DATA pendiente. Cuando un Sum
finalmente recibe su `EOF_SUM`, la garantía FIFO asegura que ya leyó y
procesó todos los DATA de esa query que le tocaban. El flush es seguro
y completo, sin importar cuántos clientes concurrentes haya ni qué tan
entrelazados estén sus mensajes.


## Particionamiento hacia Aggregators

Cuando un Sum flushea el estado de una query, distribuye las frutas
entre los Aggregators usando `hash(fruta) % AGGREGATION_AMOUNT`. Cada
fruta va a un único Aggregator, no a todos. Esto evita el broadcast
redundante del esqueleto original y asegura que todas las instancias de
una misma fruta terminen acumuladas en el mismo lugar, donde pueden
sumarse correctamente.

El hash se calcula con MD5 en lugar del `hash()` builtin de Python
porque este último varía entre procesos según `PYTHONHASHSEED` y
rompería el particionamiento entre réplicas.

Al terminar el flush de una query, cada Sum envía un `EOF(query_id)` a
cada Aggregator (incluso cuando en esa partición no le tocó ninguna
fruta). Esto simplifica la lógica del Aggregator, que no necesita saber
si un Sum le mandó datos o no: solo cuenta EOFs.


## Aggregators y Join

Cada Aggregator mantiene estado por query con los items acumulados y un
contador de EOFs recibidos. Al recibir un `EOF(query_id)` incrementa el
contador. Cuando el contador alcanza `SUM_AMOUNT`, sabe que todos los
Sums terminaron su partición para esa query, entonces ordena los items,
corta en `TOP_SIZE` y publica el top parcial al Join.

El Join hace lo análogo con tops parciales en vez de DATA:

- Mantiene por cada query una lista de items y un contador de tops
  parciales recibidos.
- Cuando el contador alcanza `AGGREGATION_AMOUNT`, concatena los items
  de todos los Aggregators, ordena, corta en `TOP_SIZE`, y publica el
  top final a `results_queue`.
- El merge es directo porque las particiones del Aggregator son
  disjuntas (gracias al hash): no hay frutas repetidas entre
  Aggregators que requieran fusión extra.

En ambos filtros el valor de `SUM_AMOUNT` y `AGGREGATION_AMOUNT` se
obtiene del environment del contenedor, por lo que la coordinación se
adapta automáticamente a la cantidad configurada.


## Escalabilidad

Atender N clientes tiene costo lineal en memoria y no requiere
coordinación extra entre ellos, porque el `query_id` actúa como
separador de estado en cada filtro. Los mensajes de distintas queries
se interleavan libremente en las colas sin interferencia.

Al agregar Sums, `SUM_AMOUNT` aumenta y `input_queue` se reparte entre
más consumers. El protocolo de EOF en dos fases escala trivialmente: el
Sum que recibe `EOF_HANDLER` publica `SUM_AMOUNT` copias, sin importar
cuántas sean.

El particionamiento por hash distribuye el trabajo de los Aggregators
en forma balanceada asumiendo distribución razonable de frutas. El
costo de control por query es lineal en los parámetros del sistema
(`SUM_AMOUNT` EOFs replicados en `input_queue`, `SUM_AMOUNT × AGGREGATION_AMOUNT`
EOFs de Sum hacia Aggregators, `AGGREGATION_AMOUNT` tops parciales de
Aggregator hacia Join, y un único top final), y no depende del tamaño
de los datos del cliente. No hay broadcasts, polling ni colas
creciendo sin bound.


## Middleware

Se implementaron las dos clases requeridas sobre pika.
`MessageMiddlewareQueueRabbitMQ` declara una cola durable compartida
con entrega round-robin entre consumers y `prefetch_count=1` para
asegurar reparto equitativo. `MessageMiddlewareExchangeRabbitMQ`
declara un exchange `direct` y publica a las routing keys
configuradas; la cola consumer se crea de forma lazy en
`start_consuming()` para evitar que una instancia creada solamente para
publicar genere colas anónimas fantasma en el broker.

El ACK manual (`auto_ack=False`) asegura semántica at-least-once: un
mensaje se confirma solo después de procesarse exitosamente, y si el
consumer muere antes de ackear, RabbitMQ lo reencola. La función
`stop_consuming` usa `add_callback_threadsafe` de pika para poder ser
invocada desde un signal handler de `SIGTERM`, habilitando graceful
shutdown en todos los servicios.