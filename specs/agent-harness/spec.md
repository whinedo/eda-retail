# Agent harness Specification

## Purpose

Reunir en una sola capa, el harness de agentes, todo lo que rodea a una llamada al LLM: el contrato de cada agente, el tratamiento de la entrada no confiable, la validación de la salida (forma, grounding y negocio), el presupuesto, el registro de cada llamada y la evaluación repetible. Materializa la frontera de la spec `llm-boundaries` («el LLM propone y el código dispone») sin cambiarla: el harness decide si una propuesta es aceptable, pero no escribe, no publica y no ejecuta efectos.

Aplica a los cuatro agentes con LLM: el intérprete (`interpreter`), el planificador (`planner`), el replanificador (`replanner`) y el redactor (`writer`, el que usan `executor_notify_customer` y `executor_file_claim`).

## Requirements

### Requirement: Contrato declarado por agente

Cada agente con LLM SHALL declarar un `AgentSpec` con su nombre, la versión de su prompt (`prompt_version`), el modelo pydantic de su salida, su comprobación de negocio, su límite de generaciones y el `max_tokens` de cada llamada. El harness MUST NOT llamar al LLM para un agente sin `AgentSpec` registrado. Cambiar el texto del prompt de sistema o de la plantilla MUST cambiar su `prompt_version`.

#### Scenario: Los cuatro agentes declaran su contrato

- **GIVEN** el registro de `AgentSpec`
- **WHEN** se listan los agentes registrados
- **THEN** están `interpreter`, `planner`, `replanner` y `writer`
- **AND** cada uno tiene una `prompt_version` no vacía

#### Scenario: Agente sin contrato

- **GIVEN** un agente `unknown_agent` sin `AgentSpec` registrado
- **WHEN** pide al harness una generación
- **THEN** el harness la rechaza con un error de configuración
- **AND** no se hace ninguna llamada al LLM

#### Scenario: Prompt cambiado sin cambiar su versión

- **GIVEN** el `AgentSpec` de un agente con su `prompt_version` y la huella de su prompt guardada en el registro
- **WHEN** cambia el texto del prompt de sistema o de la plantilla y la `prompt_version` sigue igual
- **THEN** el test de contrato de ese agente falla y dice qué agente tiene la huella desactualizada

### Requirement: El harness no escribe ni publica

El harness SHALL ejecutarse antes de abrir la transacción del componente y devolver un `AgentOutcome`: el valor aceptado o el motivo de escalado, más la lista de llamadas hechas. MUST NOT escribir en ninguna tabla, publicar en Kafka ni ejecutar efectos. Es el componente el que registra las llamadas y su resultado en su transacción.

#### Scenario: Generación sin escrituras

- **GIVEN** un planificador con el cliente simulado y una base con su esquema
- **WHEN** el harness genera un plan válido
- **THEN** devuelve el plan y sus llamadas
- **AND** `planner.plans`, `planner.llm_calls` y `planner.outbox` no tienen filas nuevas hasta que el command handler confirma su transacción

### Requirement: Entrada no confiable delimitada y acotada

El texto que viene de fuera (el aviso del transportista) SHALL ir delimitado como datos. Si el texto contiene el delimitador de cierre, el harness MUST neutralizarlo antes de construir el prompt, para que el texto no pueda salirse del bloque de datos. Un texto de entrada por encima de `MAX_UNTRUSTED_INPUT_CHARS` (por defecto 8000) MUST rechazarse sin llamar al LLM, y la incidencia se escala con el motivo `input_too_long`.

#### Scenario: Aviso que intenta cerrar el bloque de datos

- **GIVEN** un aviso cuyo texto contiene `</aviso>` seguido de instrucciones
- **WHEN** el harness construye el prompt del intérprete
- **THEN** el prompt tiene un único bloque `<aviso>` … `</aviso>`
- **AND** el `</aviso>` del texto queda neutralizado dentro del bloque

#### Scenario: Aviso demasiado largo

- **GIVEN** un aviso de 8001 caracteres con `MAX_UNTRUSTED_INPUT_CHARS` en 8000
- **WHEN** el intérprete lo atiende
- **THEN** no se hace ninguna llamada al LLM
- **AND** se publica `IncidentEscalated` con el motivo `input_too_long`

### Requirement: Grounding de las referencias

Las referencias de una salida del LLM SHALL salir de su entrada, no del modelo. Un `order_id` en la salida del intérprete MUST aparecer literalmente en el texto del aviso. Un `order_id` en los parámetros de un paso del planificador o del replanificador MUST ser uno de los pedidos de la incidencia que trae el comando. Una salida con una referencia que no cumpla esto SHALL tratarse como una generación rechazada con el motivo `ungrounded_reference`: cuenta para el límite de generaciones, el motivo se le devuelve al LLM y, agotado el límite, la incidencia se escala con `llm_output_invalid`. El grounding se comprueba antes que las reglas de negocio.

#### Scenario: Intérprete que inventa un pedido

- **GIVEN** un aviso que solo menciona el pedido `ORD-1` y un LLM que devuelve `order_ids` `["ORD-1", "ORD-9"]`
- **WHEN** el harness valida la salida
- **THEN** la rechaza con `ungrounded_reference`
- **AND** pide una nueva generación con ese motivo

#### Scenario: Plan sobre un pedido ajeno a la incidencia

- **GIVEN** una incidencia con los pedidos `ORD-1` y `ORD-2` y un plan con un `issue_refund` de `ORD-7`
- **WHEN** el harness valida el plan
- **THEN** lo rechaza con `ungrounded_reference` antes de comprobar el importe del reembolso
- **AND** no se publica `PlanCreated` con ese plan

#### Scenario: Grounding agotado

- **GIVEN** un límite de tres generaciones y tres salidas con referencias sin grounding
- **WHEN** se agota el límite
- **THEN** se publica `IncidentEscalated` con el motivo `llm_output_invalid`

#### Scenario: Pedido citado aún sin registrar

- **GIVEN** un aviso que menciona `ORD-5`, que aparece en el texto pero aún no está en `view_orders`
- **WHEN** el intérprete lo atiende
- **THEN** la salida pasa el grounding
- **AND** el aviso se aparca como hasta ahora (spec incident-interpretation)

### Requirement: Registro común de las llamadas con la versión del prompt

Los cuatro agentes SHALL registrar sus llamadas con una misma función del harness, que escribe la fila en `<esquema>.llm_calls` y el `LlmCallRecorded` en el outbox, en la transacción del componente. `LlmCallRecorded` pasa a la versión 2 con dos campos nuevos: `prompt_version` y `prompt_hash` (SHA-256 del prompt de sistema y de la plantilla, sin la entrada). El evento MUST NOT llevar el prompt renderizado ni la salida. Un `LlmCallRecorded` v1 SHALL leerse en la versión vigente con `prompt_version` `"unknown"` y `prompt_hash` nulo.

#### Scenario: Llamada rechazada registrada con su prompt

- **GIVEN** un planificador cuya primera generación se rechaza y la segunda se acepta
- **WHEN** el command handler confirma su transacción
- **THEN** hay dos `LlmCallRecorded` v2, con `attempt` 1 y 2 y `validation_passed` falso y verdadero
- **AND** los dos llevan la `prompt_version` y el `prompt_hash` del `AgentSpec` del planificador

#### Scenario: Evento v1 leído en la versión vigente

- **GIVEN** un `LlmCallRecorded` con `event_version` 1 en el log
- **WHEN** un proyector lo lee
- **THEN** lo recibe como v2 con `prompt_version` `"unknown"` y `prompt_hash` nulo

#### Scenario: Sin prompt ni salida en el log

- **GIVEN** cualquier `LlmCallRecorded` v2
- **WHEN** se inspecciona su payload
- **THEN** no contiene el prompt renderizado, el texto del aviso ni la salida del LLM

### Requirement: Grabación y reproducción de llamadas

Con `LLM_RECORD_DIR` definido, el cliente real SHALL guardar cada petición y su respuesta en un fichero JSONL de ese directorio, fuera del log y de la base, con la clave `(agent, prompt_hash, hash de la petición renderizada)`. Con `LLM_CLIENT=replay` y `LLM_REPLAY_DIR`, el cliente SHALL devolver la respuesta grabada para esa clave sin contactar con ningún modelo. Una petición sin grabación MUST fallar con el error `replay_miss`, sin llamar a un modelo real ni contarla como generación del LLM.

#### Scenario: Reproducción idéntica sin modelo

- **GIVEN** una ejecución con Ollama grabada en `LLM_RECORD_DIR` con la semilla 42
- **WHEN** se repite con `LLM_CLIENT=replay`, el mismo directorio y la misma semilla
- **THEN** cada agente obtiene las mismas salidas, en el mismo orden y con los mismos motivos de rechazo
- **AND** no se abre ninguna conexión con Ollama

#### Scenario: Petición sin grabar

- **GIVEN** un directorio de reproducción sin la petición de un aviso nuevo
- **WHEN** el intérprete atiende ese aviso con `LLM_CLIENT=replay`
- **THEN** la llamada falla con `replay_miss`
- **AND** no hay ningún `LlmCallRecorded` de esa llamada

#### Scenario: Grabación de otra versión del prompt

- **GIVEN** una grabación hecha con la `prompt_version` 3 del planificador
- **WHEN** se reproduce con la `prompt_version` 4
- **THEN** las peticiones del planificador fallan con `replay_miss`, porque su `prompt_hash` es distinto

### Requirement: Evaluación por agente

`eda-agent-eval <directorio>` SHALL reproducir un directorio grabado con el harness y dar, por agente y `prompt_version`: la tasa de salidas aceptadas al primer intento, la media de intentos, los rechazos por motivo (`ungrounded_reference`, esquema y cada regla de negocio), los escalados por motivo y los tokens de entrada y salida. No escribe en la base ni en Kafka.

#### Scenario: Informe de un directorio grabado

- **GIVEN** un directorio grabado con 20 incidencias en el que el planificador acepta 15 planes al primer intento y 5 al segundo
- **WHEN** se ejecuta `eda-agent-eval` sobre él
- **THEN** el informe del planificador da una tasa al primer intento de 0,75 y una media de 1,25 intentos
- **AND** la base y el topic no tienen filas ni mensajes nuevos
