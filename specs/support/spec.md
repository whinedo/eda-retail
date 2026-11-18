# Support Specification

## Purpose

Dar a soporte técnico una herramienta para diagnosticar el sistema y recuperarlo tras un problema (pérdida del topic, eventos que no llegaron, una vista desfasada) sin escribir SQL ni llamar a funciones internas a mano. Es el CLI `eda-support`, que el front lanza desde su vista de soporte (spec web-front). Solo ofrece procedimientos que el sistema ya tiene (`outbox.republish_in_order`, `ComponentRelay.replay_from`, la reconstrucción de vistas) más un diagnóstico de solo lectura; reprocesar dead letters y reintentar trabajos fallidos quedan fuera.

`eda-support` es una herramienta de mantenimiento, no un componente: para recuperar el sistema escribe en tablas de otros esquemas (las filas de `outbox`, las posiciones del lado de lectura en `event_log.offsets` y `views.offsets`, las vistas que reconstruye). Es la única excepción a «cada componente escribe solo en sus tablas» (spec event-backbone, «Propiedad de los datos»), y por eso cada acción que escribe exige su condición previa (normalmente, el sistema o el componente parado) y una confirmación explícita. Su propio registro vive en su esquema, `support`.

## Requirements

### Requirement: Diagnóstico

`eda-support status` SHALL informar, sin escribir nada, del estado que hace falta para decidir una recuperación: los procesos vigilados (spec web-front, «Mapa de sistemas») vivos y caídos; por partición de `eda.events`, sus marcas de agua baja y alta; los miembros y el lag de cada grupo de consumidores de Kafka (`interpreter`, `orchestrator`); el lag del lado de lectura (`event_log` y cada proyector) desde sus posiciones guardadas; por componente con outbox, las filas sin publicar; por esquema, las dead letters; los trabajos `failed` de cada componente con trabajos largos; y los avisos aparcados. Con `--json` SHALL escribir lo mismo en un único objeto JSON.

El diagnóstico SHALL incluir una lista `alerts` con un código por problema detectado: `topic_lost` cuando el topic tiene menos mensajes de los que recuerdan los outbox (en alguna partición, el offset más alto guardado en los outbox no es menor que la marca de agua alta, o el más bajo es menor que la marca baja), `outbox_pending`, `dead_letters`, `jobs_failed` y `parked_notices` cuando su cuenta no es 0, y `kafka_down` o `postgres_down` cuando no responden. Si un servicio no responde, el resto del diagnóstico SHALL seguir saliendo.

#### Scenario: Diagnóstico de un sistema sano

- **GIVEN** un sistema con todos los outbox publicados y el topic intacto
- **WHEN** se ejecuta `eda-support status --json`
- **THEN** la salida es un objeto JSON con los procesos, el topic, los grupos, el lado de lectura, los outbox, las dead letters, los trabajos fallidos y los avisos aparcados
- **AND** `alerts` no contiene `topic_lost`
- **AND** no se ha escrito nada en la base de datos

#### Scenario: Topic perdido

- **GIVEN** outbox cuyas filas recuerdan offsets en `eda.events` que el topic ya no tiene
- **WHEN** se ejecuta `eda-support status`
- **THEN** `alerts` contiene `topic_lost`

#### Scenario: Kafka no responde

- **GIVEN** Kafka caído y PostgreSQL disponible
- **WHEN** se ejecuta `eda-support status --json`
- **THEN** `alerts` contiene `kafka_down`
- **AND** la salida sigue dando los outbox, las dead letters, los trabajos fallidos y los avisos aparcados

### Requirement: Simulación y confirmación

Cada acción que escribe (`republish`, `replay-from`, `rebuild`) SHALL ejecutarse por defecto como simulación: comprueba sus condiciones previas, describe lo que haría (filas, particiones, grupos, tablas) y da un token de confirmación, sin escribir nada. Solo con `--confirm <token>` SHALL ejecutarse. El token SHALL derivarse de la acción, sus argumentos y el estado observado: si el estado cambia entre la simulación y la ejecución, el token ya no vale y la acción se niega sin escribir. Una condición previa que no se cumple SHALL negar la acción, también en la simulación, con un mensaje que diga cuál y por qué.

Los códigos de salida SHALL ser: 0 si la simulación o la ejecución terminan bien, 1 si la ejecución falla y 2 si la acción se niega (condición previa o token).

#### Scenario: Simulación sin efectos

- **GIVEN** una acción que escribe con sus condiciones previas cumplidas
- **WHEN** se ejecuta sin `--confirm`
- **THEN** describe lo que haría, da un token y termina con 0
- **AND** no se ha escrito nada

#### Scenario: Confirmación con el token de la simulación

- **GIVEN** el token de una simulación y el mismo estado
- **WHEN** se ejecuta la acción con `--confirm <token>`
- **THEN** la acción se ejecuta y termina con 0

#### Scenario: Token caducado

- **GIVEN** el token de una simulación y un estado que ha cambiado después (por ejemplo, una fila nueva en el outbox)
- **WHEN** se ejecuta la acción con `--confirm <token>`
- **THEN** se niega con código 2, dice que el estado cambió desde la simulación y no escribe nada

#### Scenario: Condición previa sin cumplir

- **GIVEN** una acción cuya condición previa no se cumple
- **WHEN** se simula o se ejecuta
- **THEN** se niega con código 2 y dice qué condición falta

### Requirement: Sistema parado

«Sistema parado» SHALL comprobarse, no suponerse: ningún proceso vigilado vivo y ningún miembro activo en los grupos de Kafka `interpreter` y `orchestrator`.

#### Scenario: Un proceso sigue vivo

- **GIVEN** el proceso del orquestador vivo
- **WHEN** se simula una acción que exige el sistema parado
- **THEN** se niega con código 2 y nombra el proceso vivo

#### Scenario: Un grupo de Kafka tiene miembros

- **GIVEN** ningún proceso vigilado vivo y un miembro activo en el grupo `interpreter`
- **WHEN** se simula una acción que exige el sistema parado
- **THEN** se niega con código 2 y nombra el grupo

### Requirement: Republicación tras perder el topic

`eda-support republish --mode truncated|recreated` SHALL republicar el outbox de todos los componentes en el orden que tenía cada partición (`outbox.republish_in_order`, spec event-backbone, «Pérdida del topic»). Exige el sistema parado y un topic vacío que case con el modo: en `truncated`, cada partición con su marca baja igual a la alta (vaciado con `delete_records`, los offsets siguen); en `recreated`, todas las marcas altas a 0 (borrado y creado otra vez). En `recreated`, antes de republicar, SHALL llevar a 0 los offsets de los grupos `interpreter` y `orchestrator` y poner a -1 la posición guardada del lado de lectura (`event_log.offsets` y `views.offsets`) para ese topic. La simulación SHALL dar las filas a republicar por componente y, en `recreated`, los grupos y las posiciones que reinicia.

#### Scenario: Republicación de un topic vaciado

- **GIVEN** el sistema parado, el topic vaciado con `delete_records` y outbox con filas
- **WHEN** se ejecuta `eda-support republish --mode truncated --confirm <token>`
- **THEN** se republican todas las filas de los outbox en el orden de `republish_order` y la salida da las filas por componente
- **AND** no se tocan los offsets de los grupos ni las posiciones del lado de lectura

#### Scenario: Republicación de un topic recreado

- **GIVEN** el sistema parado, el topic borrado y creado de nuevo (marcas altas a 0) y outbox con filas
- **WHEN** se ejecuta `eda-support republish --mode recreated --confirm <token>`
- **THEN** los grupos `interpreter` y `orchestrator` quedan en el offset 0 y las posiciones del lado de lectura en -1 para ese topic
- **AND** después se republican todas las filas de los outbox en el orden de `republish_order`

#### Scenario: Topic que no está vacío

- **GIVEN** el sistema parado y un topic con mensajes
- **WHEN** se simula `eda-support republish` en cualquier modo
- **THEN** se niega con código 2: republicar sobre un topic con mensajes los duplicaría en otro orden

#### Scenario: Modo que no casa con el topic

- **GIVEN** el sistema parado y un topic vaciado con `delete_records` (marcas altas mayores que 0)
- **WHEN** se simula `eda-support republish --mode recreated`
- **THEN** se niega con código 2 y dice que el topic parece vaciado, no recreado

### Requirement: Republicación desde una fila

`eda-support replay-from <componente> <outbox_id>` SHALL marcar como pendientes las filas del outbox del componente desde `outbox_id` (`ComponentRelay.replay_from`), para que su relay las publique otra vez. No exige el sistema parado: los consumidores absorben lo ya aplicado con el inbox. El componente SHALL ser uno con outbox y la fila SHALL existir. La simulación SHALL dar cuántas filas se marcarían y el intervalo de `id`.

#### Scenario: Filas marcadas para publicarse otra vez

- **GIVEN** el outbox de `planner` con filas publicadas desde `outbox_id`
- **WHEN** se ejecuta `eda-support replay-from planner <outbox_id> --confirm <token>`
- **THEN** esas filas quedan con `published_at` nulo y la salida da cuántas son
- **AND** las filas anteriores a `outbox_id` no cambian

#### Scenario: Componente o fila desconocidos

- **GIVEN** un componente sin outbox o un `outbox_id` que no existe
- **WHEN** se simula `eda-support replay-from`
- **THEN** se niega con código 2 y dice cuál falta

### Requirement: Reconstrucción de una vista

`eda-support rebuild <vista>` SHALL reconstruir la vista como `eda-projector rebuild` (spec cqrs-views, «Reconstrucción de vistas») y dar las huellas antes y después. Exige el proceso de los proyectores parado. La simulación SHALL dar las tablas de la vista y sus filas.

#### Scenario: Reconstrucción con los proyectores parados

- **GIVEN** el proceso de los proyectores parado
- **WHEN** se ejecuta `eda-support rebuild <vista> --confirm <token>`
- **THEN** la vista se reconstruye y la salida da las huellas antes y después y si coinciden

#### Scenario: Proyectores en marcha

- **GIVEN** el proceso de los proyectores vivo
- **WHEN** se simula `eda-support rebuild <vista>`
- **THEN** se niega con código 2: otro proceso escribiría en la vista mientras se reconstruye

### Requirement: Registro de las ejecuciones

Cada ejecución confirmada (no las simulaciones) SHALL quedar en `support.runs`: acción, argumentos, token, origen (`cli` o `web`), inicio, fin, resultado (`succeeded`, `failed` o `refused`) y un resumen en JSON. Si PostgreSQL no responde, la acción no se ejecuta. El registro MUST NOT guardar datos personales.

#### Scenario: Ejecución registrada

- **GIVEN** una acción confirmada que termina bien
- **WHEN** se consulta `support.runs`
- **THEN** hay una fila con su acción, sus argumentos, `succeeded` y su resumen

#### Scenario: Simulación sin registro

- **GIVEN** una simulación
- **WHEN** se consulta `support.runs`
- **THEN** no hay ninguna fila nueva
