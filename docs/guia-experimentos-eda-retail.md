# Guía de experimentos de eda-retail

Experimentos actuales (E1–E7) y experimentos de resiliencia e idempotencia propuestos (E8–E23)

## Índice

1. Qué se mide y cómo se ejecuta
2. Experimentos actuales (E1–E7)
3. Experimentos nuevos: catálogo (E8–E23)
4. Invariantes comunes
5. Experimentos nuevos en detalle
6. Limitaciones del arnés y orden de implantación

## 1. Qué se mide y cómo se ejecuta

La PoC de eda-retail gestiona incidencias logísticas con una arquitectura dirigida por eventos: Kafka (KRaft, un broker, topic `eda.events` con 3 particiones, retención infinita y clave `incident_id`), PostgreSQL con un schema por componente, outbox con relay, inbox por consumidor, `effect_keys` en los ejecutores y una saga (orquestador) que invoca comandos REST con plazos durables. Los experimentos ponen a prueba esas garantías con datos sintéticos y fallos inyectados, y cada uno contrasta una hipótesis.

**Cómo se ejecuta.** Un solo comando por experimento:

```
uv run eda-experiment <exp> --seed N
```

Opciones útiles: `--incidents` (tamaño del escenario), `--out` (carpeta de informes), y solo en E5 y E6 `--llm oracle|ollama`, `--repetitions`, `--temperature` y (solo E5) `--threshold`. Cada ejecución crea y borra su propia base de datos y su propio topic, y escribe `experiments/results/<exp>-<fecha>/report.json` y `report.md` con la semilla, la configuración, el modelo y la versión de los prompts. El código sale con 0 si todas las hipótesis se cumplen.

**Con informe de resultados (`eda-expexecutor`).** El expexecutor (spec `expexecutor`) es un agente determinista, sin LLM, que ejecuta el experimento con `eda-experiment` y documenta la ejecución:

```
uv run eda-expexecutor run <exp> [--seed N] [--incidents N] [--config experiments/config.py]
uv run eda-expexecutor report experiments/results/<exp>-<fecha>
uv run eda-expexecutor delete experiments/results/<exp>-<fecha>
uv run eda-expexecutor delete --all
```

- `run` guarda la salida del experimento en `experiment.log`, dentro de su carpeta, y escribe `results.md` con el propósito, el resultado (esperado, obtenido y si coinciden) y las evidencias, cada una con su origen (clave de `report.json` o línea de `experiment.log`). Termina con el mismo código que `eda-experiment`; si el experimento falla sin escribir `report.json`, el informe lo dice y cita el final del log.
- Los parámetros salen de la línea de comandos, de `experiments/config.py` (diccionario `EXPERIMENTS` por experimento) o del valor por defecto de `eda-experiment`, en ese orden, y el informe dice de dónde sale cada uno. El fichero se valida antes de ejecutar nada.
- `report` regenera `results.md` sin repetir la ejecución; `delete <carpeta>` borra solo ese `results.md`, y `delete --all` borra todas las carpetas de ejecución de `experiments/results` (no toca `experiments/config.py`). Los resultados no están en el control de versiones.

**Arnés actual.** Monta los componentes en un solo proceso contra PostgreSQL y Kafka reales: relays, proyectores, agentes, orquestador y ejecutores se atienden por turnos, y lo que tarda (el LLM, los servicios) corre en su propio hilo. La inyección de fallos (`FaultPlan`) admite hoy: duplicados de entrega (1 a N), retraso de la entrega, probabilidad de fallo por tipo de paso, caída de un consumidor tras K mensajes sin confirmar el último offset, almacenes sin stock, reinicio de los offsets de los grupos de consumidores al principio del topic (E21) y reenvío de avisos del transportista, en orden o a la vez (E23). Los mensajes veneno (E18) se publican aparte, directamente en el topic (`synthetic/poison.py`). La elección de los pasos que fallan depende de la semilla, la acción y la `idempotency_key`, no del orden de ejecución, así que es reproducible.

**Referencias de componentes** (para leer los informes): `PENDING_NOTICE_TIMEOUT_S` (300 s) aparca avisos; `SAGA_STEP_TIMEOUT_S` (60 s) y `SAGA_PLAN_TIMEOUT_S` (300 s) son los plazos de la saga; `COMMAND_HTTP_TIMEOUT_S` (5 s) es el timeout HTTP del dispatcher; `EXECUTOR_MAX_CONCURRENCY` (5) limita a los ejecutores; `JOB_LEASE_SECONDS`, `JOB_MAX_ATTEMPTS` y el backoff gobiernan los trabajos diferidos.

## 2. Experimentos actuales (E1–E7)

| Exp. | Hipótesis | Qué prueba | Escenario por defecto | Estado |
|---|---|---|---|---|
| E1 | H1 | Duplicados en el bus | 50 incidencias, cada evento y comando 2 y 3 veces | Cumplida |
| E2 | H2 | Caída de un ejecutor tras confirmar y antes de responder | 50 incidencias | Cumplida |
| E3 | H3 | Fallo de un paso y replanificación | 30 pérdidas, almacenes sin stock | Cumplida |
| E4 | H4 | Reconstrucción de vistas desde Kafka | 200 incidencias | Cumplida |
| E5 | H5 | Calidad del plan del LLM | 40 incidencias, 3 repeticiones, umbral 0,8 | No cumplida |
| E6 | H6 | Eventos frente a versión síncrona | 50 incidencias, 3 repeticiones | Informativa |
| E7 | H7 | Auditabilidad | 50 incidencias, 50 % de fallos en `reserve_stock` | Cumplida |

### E1 Duplicados en el bus (H1)

Entrega cada evento a cada consumidor y cada comando de la saga 2 y 3 veces con el mismo `command_id`, y compara tres modos de idempotencia: `full` (inbox, `effect_keys` y servicios con clave), `no_effect_keys` (los ejecutores e `inventory` pierden inbox de comandos, `effect_keys` y la deduplicación de los servicios) y `none` (todo desactivado, solo observación). **H1:** con idempotencia, 0 efectos duplicados en todas las ejecuciones; sin `effect_keys`, más de 0. Un duplicado es una ejecución externa repetida del mismo paso o una reserva de más. **Mide:** cierres, abiertas, escaladas, duplicados por tipo, caídas.

### E2 Caída de un ejecutor (H2)

El ejecutor de `notify_customer` (todas las incidencias lo usan) cae tras completar la mitad de sus pasos: confirma la transacción del último y cae antes de responder. La saga reinvoca con el mismo `command_id` y el inbox devuelve la respuesta guardada. **H2:** todas las incidencias terminan en `IncidentResolved` o `IncidentEscalated` y no se repite ningún paso. **Mide:** latencia por incidencia, caídas, pasos repetidos.

### E3 Fallo de un paso y replanificación (H3)

Solo incidencias de pérdida (`loss`: aviso, reclamación, reserva y reenvío) con los dos almacenes sin existencias. La reserva falla, el replanner quita reserva y reenvío y propone un reembolso. **H3:** todas las incidencias terminan con un reembolso o un escalado, ningún paso completado se repite y ningún reembolso supera el importe del pedido ni el máximo configurado.

### E4 Reconstrucción de vistas (H4)

Ejecuta las incidencias hasta cerrarlas y, con el sistema parado, borra cada vista y la reconstruye volviendo a consumir el topic. **H4:** la huella de cada vista reconstruida coincide con la original. **Mide:** eventos aplicados y segundos por vista.

### E5 Calidad del plan (H5)

El planificador aislado recibe la interpretación etiquetada del generador (causa, gravedad, retraso) y los pedidos, con el mismo prompt, las mismas comprobaciones (`validate_plan`) y los mismos reintentos que el agente. Un plan acierta si propone las mismas acciones que el esperado, sin importar el orden ni los `step_id`. Con gravedad `low` el plan lo fija el código y esas incidencias no cuentan. **H5:** la media del acierto de las repeticiones alcanza 0,8. **Resultado:** con Ollama (qwen2.5:7b-instruct) el acierto fue de 0,75 a 0,78 y H5 no se cumple. Errores sistemáticos: en `strike` añade `file_claim`; en `delay` de gravedad alta añade `reserve_stock` y `reissue_shipment`.

### E6 Eventos frente a síncrono (H6)

Cada repetición ejecuta los mismos avisos, con el mismo LLM, en la versión de eventos y en la base síncrona (`baseline_sync`), en dos escenarios: `no_faults` y `faults` (cada evento entregado 2 veces y un 30 % de fallos en `reserve_stock`). El informe da latencia (media, mediana, p95), tokens, llamadas, coste, incidencias resueltas, reembolsadas y escaladas, efectos duplicados y escaladas por motivo. **H6** no fija umbral: describe diferencias. **Resultado destacado:** ambas versiones con 0 duplicados; latencia de eventos de unos 1019 s frente a unos 32 s en síncrono, porque con un único Ollama las 50 incidencias simultáneas hacen cola en el LLM; todas las escaladas fueron `llm_output_invalid`. Los plazos de la saga se escalan con las incidencias en este experimento y el informe lo declara.

### E7 Auditabilidad (H7)

Ejecuta con un 50 % de fallos en `reserve_stock` para que haya planes revisados y explica, desde el log y la traza (cadena de `causation_id`), diez incidencias elegidas con la semilla. Cada decisión es un `StepRequested` de `audit_trail`; su explicación necesita agente, versión del plan y motivo. **H7:** todas las decisiones de las incidencias elegidas tienen los tres.

**Cobertura.** E1 a E4 y E7 prueban idempotencia, caída de un ejecutor, replan, reconstrucción y auditoría en condiciones de laboratorio benignas; no prueban caídas de Kafka o de PostgreSQL, procesos congelados, mensajes envenenados, proyecciones atrasadas ni reproducción completa del log. Los experimentos nuevos cubren esos huecos.

## 3. Experimentos nuevos: catálogo (E8–E23)

Las hipótesis nuevas se llaman como su experimento (H11, H12...). Los marcados con (S) tienen spec completa en `specs-experimentos-resiliencia.md`.

| Exp. | Nombre | Fallo inyectado | Qué se comprueba | Modo |
|---|---|---|---|---|
| E8 | Matriz de puntos de caída | Caída de cada componente en cada punto de la transacción | Recuperación sin duplicados ni pérdidas | En proceso |
| E9 | Caída de Kafka | Broker parado y reiniciado | El outbox acumula y el relay publica al volver, sin pérdidas | Multiproceso |
| E10 | Caída de PostgreSQL | Conexiones cortadas o base parada | Reintentos, sin estado a medias | Multiproceso |
| E11 (S) | Entrega retrasada y replanes | Resultados tardíos, 50 % de fallos en `reserve_stock` | Eventos obsoletos ignorados, compensaciones correctas | Implementado |
| E12 (S) | Ejecutor caído y colgado | `file_claim` en `down` y en `hang` | Plazos, aislamiento, cabeza de línea del dispatcher | Multiproceso |
| E13 | Fallo de compensación | `release_stock` falla | Escalado `compensation_failed`, sin reembolso | En proceso |
| E14 | Ráfaga y contrapresión | Pico de avisos, 503 de los ejecutores | Dispatcher y cola de trabajos se recuperan | En proceso |
| E15 | Degradación del LLM | Latencia o salidas inválidas | `llm_output_invalid`, plazos, trabajos reintentados | En proceso |
| E16 (S) | Consumidor zombie | `SIGSTOP` más allá de la sesión y rebalanceo | Sin transiciones inválidas ni comandos duplicados | Multiproceso |
| E17 (S) | Proyección atrasada | Proyectores parados | Avisos aparcados, reanudación y escalado | Implementado |
| E18 (S) | Mensajes veneno | Mensajes inválidos en mitad de la partición | DLQ sin bloquear la partición | Implementado |
| E19 | Pérdida del topic | Topic vaciado y republicación desde el outbox | Se recupera el log sin repetir efectos | En proceso |
| E20 | Dependencias de V2 | MCP de correo y agente A2A inestables | Timeouts, 401, tarjeta inválida | En proceso |
| E21 (S) | Reproducción total del log | Offsets de todos los grupos al principio | 0 efectos, eventos y cambios de estado nuevos | En proceso |
| E22 (S) | Carreras de comandos y ventana efecto–registro | Entrega concurrente y caída entre efecto y clave | 0 efectos duplicados | En proceso |
| E23 (S) | Duplicados de negocio | Avisos reenviados y replanes que repiten acciones | Máximos de las specs de idempotencia | En proceso |

## 4. Invariantes comunes

Todo experimento de resiliencia comprueba, además de su hipótesis, estos cinco invariantes y los incluye en el informe:

| Id | Invariante |
|---|---|
| I1 | Toda incidencia termina en `IncidentResolved` o `IncidentEscalated` |
| I2 | Ningún efecto externo se ejecuta dos veces para la misma `idempotency_key` |
| I3 | Como máximo un `issue_refund` por incidencia y pedido, y nunca con compensaciones pendientes |
| I4 | Toda fila de `outbox` está publicada o pendiente; ninguna se pierde |
| I5 | Las vistas reconstruidas desde `eda.events` tienen la misma huella que las mantenidas |

Si I1 o I2 se violan, la hipótesis no se da por cumplida aunque su criterio propio sí lo esté.

**Implementación.** `experiments/invariants.py` los comprueba al terminar, con el sistema parado: I1 con el log (incidencias con `CarrierNoticeReceived` sin `IncidentResolved` ni `IncidentEscalated`), I2 con los efectos repetidos que mide E1, I3 con `executor_issue_refund.refunds` y con el orden de los `StepRequested` y `StepCompleted` del log (un `issue_refund` pedido con una compensación pedida y sin completar), I4 con cada `outbox` (filas sin publicar o cuyo evento no está en `event_log.events`) e I5 reconstruyendo cada vista desde el topic. E11, E17, E18, E21 y E23 los incluyen en `metrics.invariants`; E21 los comprueba antes de sus observaciones y de su control, que repiten efectos a propósito.

## 5. Experimentos nuevos en detalle

### E11 Entrega retrasada y replanes forzados (H11)

- **Objetivo.** Comprobar que los resultados de una `plan_version` anterior, o posteriores al plazo, no cambian el estado de la saga.
- **Escenario.** 50 incidencias, 50 % de fallos en `reserve_stock`, planes con pasos en paralelo y retraso del resultado (`step_delay_seconds`) en `reserve_stock`, `reissue_shipment` y `file_claim`. Como Kafka mantiene el orden por incidencia dentro de la partición, los resultados obsoletos nacen del paralelismo y de los plazos, no del reordenamiento del bus.
- **Mide.** `PlanRevised` por incidencia, resultados obsoletos por versión y tras el plazo, los que cambiaron el estado de la saga fuera del supuesto A4 y las compensaciones A4, compensaciones pedidas y completadas, escaladas por motivo e I1 a I5.
- **Criterio.** H11: I1 a I5 sin violaciones, ningún resultado obsoleto cambia el estado de la saga salvo la excepción A4 (el `StepCompleted` tardío de un `reserve_stock` cancelado se marca completado y pide su `release_stock`) y ninguna compensación se pide más de una vez.
- **Implementación.** `uv run eda-expexecutor run e11`. El orquestador descarta los obsoletos sin registrarlos, así que se cuentan desde el log: obsoleto por versión si su `plan_version` es anterior a la vigente al consumirlo; tras el plazo si el harness anotó que su plazo venció con el paso pedido (el plazo no deja evento). Un obsoleto cambia el estado si el orquestador emite algún evento con él como causa. Retrasos base: `reserve_stock` 1 s, `reissue_shipment` 2 s y `file_claim` 6 s (jitter de 0,5 a 1,5), con un plazo de `ExecuteStep` de 4 s. El retraso deja ejecutar el efecto a su hora y añade su resultado al outbox del ejecutor más tarde.

### E12 Ejecutor caído y ejecutor colgado (H12)

- **Objetivo.** Comprobar la vivacidad ante un ejecutor permanentemente fuera y medir el aislamiento entre incidencias.
- **Escenario.** Multiproceso, 50 incidencias (la mitad con `file_claim`), `file_claim` en `down`, en `hang` (acepta la conexión y no responde) y en `hang` con recuperación.
- **Hipótesis a contrastar.** El dispatcher invoca un comando por turno (`LIMIT 1`): con `hang`, cada intento ocupa `COMMAND_HTTP_TIMEOUT_S` (5 s) y puede retrasar los comandos de otras incidencias (cabeza de línea). Es una suposición a verificar, no un hecho conocido.
- **Mide.** Intentos por comando, tiempo del dispatcher esperando al ejecutor, latencia p95 de las incidencias que no usan `file_claim`, escaladas por `step_timeout`, resultados obsoletos tras la recuperación.
- **Criterio.** H12a (vivacidad, obligatoria): todo termina dentro de `SAGA_STEP_TIMEOUT_S` más margen. H12b (aislamiento, exploratoria): p95 de las no afectadas ≤ 2 × el de una ejecución sin fallo; un resultado negativo es un hallazgo que se documenta.
- **Necesita.** Modo multiproceso y modos `down`/`hang` de ejecutor.

### E16 Consumidor zombie y rebalanceo (H16)

- **Objetivo.** Sondear el supuesto de «único dueño del estado de la saga, sin concurrencia optimista» cuando dos instancias coexisten por un rebalanceo.
- **Escenario.** Multiproceso, dos instancias del orquestador en el mismo grupo, 50 incidencias en curso, `SIGSTOP` a la que posee más particiones más tiempo que `session.timeout.ms`, luego `SIGCONT`.
- **Mide.** Instante del rebalanceo, mensajes que procesó la zombie tras reanudar, solapamientos por incidencia, esperas al bloqueo de la incidencia, descartes del inbox, comandos con `command_id` distinto para el mismo paso.
- **Criterio.** H16: sin transiciones inválidas, sin retrocesos de `completed` y sin comandos duplicados con `command_id` distinto; I1 a I5 sin violaciones.
- **Necesita.** Modo multiproceso y congelación de procesos.

### E17 Proyección atrasada y avisos aparcados (H17)

- **Objetivo.** Verificar el tratamiento explícito de la consistencia eventual: el intérprete no sondea la vista; aparca el aviso con plazo y reanuda con el evento.
- **Escenario.** Proyectores parados al llegar los avisos, 50 avisos, `PENDING_NOTICE_TIMEOUT_S` en 30 s. Un aviso aparcado no se reanuda al reanudar los proyectores: se reanuda al consumir de Kafka el `OrderRegistered` de su pedido o, si ese evento llegó antes de aparcarlo, al vencer el plazo consultando la vista una vez. Por eso las variantes se definen por el orden de los eventos: (a) pedidos registrados después de aparcar los avisos (se reanudan por `OrderRegistered` con los proyectores aún parados); (b) pedidos registrados antes y proyectores parados más que el plazo (escalan con `no_valid_orders`); (c) pedidos registrados antes y proyectores reanudados antes del plazo (se reanudan por el plazo con el pedido proyectado). En (a) se reenvía además un aviso aparcado.
- **Mide.** Por variante: aparcados, reanudados por `OrderRegistered`, reanudados por plazo con pedido proyectado, escalados con `no_valid_orders`, tiempo en aparcamiento, avisos que quedan aparcados (`interpreter.pending_notices` y `parked_notices`), pedidos en `view_orders` con los proyectores parados y lag del intérprete con los avisos aparcados.
- **Criterio.** H17: (a) todos reanudados por `OrderRegistered`, (b) todos escalados, (c) todos reanudados por el plazo sin escalar; ninguno aparcado al final, lag 0 del intérprete con los avisos aparcados y el aviso reenviado descartado con `duplicate_notice` sin un segundo aparcamiento.
- **Implementación.** `uv run eda-expexecutor run e17`. Cada variante usa su propia base y su topic. El harness para y reanuda los proyectores (`pause_projectors`, `resume_projectors`; `event_log` sigue consumiendo) y siembra los pedidos cuando lo pide el experimento (`RunConfig.seed_orders=False` y `seed_orders(wait=False)`). Las cuentas salen del log: un `ShipmentDelayed` de un aviso aparcado cuya causa es un `OrderRegistered` es una reanudación por el pedido; si no, por el plazo.

### E18 Mensajes veneno en mitad de la partición (H18)

- **Objetivo.** Comprobar que un mensaje inválido ni detiene al consumidor ni se pierde.
- **Escenario.** 100 incidencias; se inyectan en `eda.events`, en posiciones distintas: JSON inválido, evento sin un campo obligatorio de la estructura común (`producer`), `event_type` desconocido, `event_version` sin upcaster, payload fuera de schema, payload con datos personales y payload de 900 KB.
- **Mide.** Por tipo y por consumidor: presencia en `<componente>.dead_letters` y en `eda.dlq`, motivo, lag final, reintentos antes de saltar.
- **Criterio.** H18: lag final 0 en todos los grupos, todo veneno en la DLQ de cada consumidor que atiende su tipo exactamente una vez (y en ninguna más), el `event_type` desconocido en `event_log.events` sin dead letter, ninguna otra dead letter y las 100 incidencias terminan.
- **Implementación.** `uv run eda-expexecutor run e18`. Los venenos de tipo legible son `ShipmentDelayed` con el `correlation_id` de una incidencia abierta (se inyectan cuando el orquestador ya tiene incidencias abiertas), repartidos por las particiones; los atienden el orquestador, `event_log` y los proyectores con handler para ese tipo, y el JSON inválido lo atienden todos. El lado de lectura guarda sus dead letters en `event_log.dead_letters` y `views.dead_letters` (migración 0024). La reconciliación se hace con las filas de cada `dead_letters` (`published_at` indica que el relay lo llevó a la DLQ) y el informe da también los mensajes del topic DLQ y si cada incidencia afectada se escaló con `invalid_schema` (solo se escala si sigue abierta al leer el veneno).
- **Nota.** El consumidor trata de forma distinta un mensaje que incumple el schema (va a la DLQ y se confirma el offset) y un fallo del handler (la excepción se propaga y el offset no se confirma); el experimento observa también el segundo caso, que es el que puede producir un bucle de reintentos.

### E21 Reproducción total del log (H21)

- **Objetivo.** Demostrar que reprocesar todo el log, por error o por una recuperación, no repite ningún efecto (spec `idempotency`: «Reproducción del log con ejecutores activos»).
- **Escenario.** 50 incidencias cerradas con 30 % de fallos en `reserve_stock` (hay revisiones y compensaciones). Con el sistema parado se llevan los offsets de todos los grupos a 0 y se reinicia hasta lag 0. Variante de control `no_effect_keys`: los `ExecuteStep` antiguos se reentregan a los ejecutores sin inbox de comandos ni `effect_keys`. Como observación, se reproduce además sin el inbox del orquestador y luego sin el del intérprete.
- **Mide.** Ejecuciones de cada servicio simulado por clave, filas de cada `outbox`, `inbox` y `command_outbox`, huella de `plan_steps` y de las vistas, mensajes descartados por inbox; además, `ExecuteStep` antiguos reentregados a los ejecutores activos.
- **Criterio.** H21: nada cambia con el inbox activo y el control produce efectos repetidos (demuestra que el experimento detecta la falta de idempotencia).
- **Hallazgo.** Apagar el inbox de un solo consumidor no repite nada: la saga descarta por estado los eventos de incidencias cerradas y los resultados obsoletos, y el intérprete deduplica el aviso con el inbox de comandos de `OpenIncident`. Por eso el control está en la capa de efectos y la variante `inbox_off` queda como observación.
- **Necesita.** Reinicio de offsets de un grupo.
- **Diferencia con E1.** E1 duplica en caliente durante una ejecución; E21 reproduce el log completo después, con todo el estado ya escrito.
- **Implementación.** `uv run eda-expexecutor run e21`. Los grupos de Kafka (intérprete y orquestador) vuelven al offset 0 con el sistema parado; el lado de lectura guarda su posición en PostgreSQL (`views.offsets`), así que se rebobina desde el offset 0 y descarta lo ya aplicado. Los `ExecuteStep` antiguos se reentregan con el sistema en marcha y las incidencias ya cerradas (duplicado tardío). Las observaciones `inbox_off` y el control `no_effect_keys` (el último, porque quita las restricciones de unicidad de negocio) se ejecutan sobre la misma base después de la reproducción con inbox, y cada uno se compara con la foto anterior.

### E22 Carreras de comandos y ventana efecto–registro (H22)

- **Objetivo.** Ejercer lo que E1 no toca: carreras reales sobre el inbox y `effect_keys`, y la caída entre el efecto externo y el registro de su clave.
- **Variantes.** (a) mismo `ExecuteStep` desde 2, 4 y 8 hilos con conexiones propias; (b) mismo `CreatePlan`, `RevisePlan`, `RegisterCarrierNotice` y `SeedSampleData` concurrentes; (c) dos `command_id` con la misma `idempotency_key`; (d) caída tras enviar el correo o emitir el reembolso y antes de registrar la clave, con reintento.
- **Mide.** Ejecuciones del servicio por clave, respuestas distintas para un mismo `command_id`, eventos escritos, esperas por clave `in_progress` y recuperación tras `locked_until`.
- **Observación sin hipótesis.** Un mismo `command_id` con payload distinto: la spec no lo define; el informe lo anota y propone cerrar el caso.
- **Criterio.** H22: como mucho una ejecución del servicio por clave y la misma respuesta para todas las entregas de un `command_id`.
- **Necesita.** Entrega concurrente de comandos y punto de caída `crash_between_effect_and_record`.

### E23 Duplicados de negocio (H23)

- **Objetivo.** Cubrir los duplicados que no vienen del bus sino del negocio: avisos reenviados por el transportista, replanes que repiten acciones.
- **Variantes.** (a) cada aviso reenviado de 1 a 5 veces, en orden y a la vez; (b) replanes que vuelven a pedir `file_claim` y `notify_customer`; (c) reembolso pedido otra vez en un plan posterior; (d) el mismo pedido en dos incidencias.
- **Mide.** Un `incident_id` por `carrier_notice_id`, descartes `duplicate_notice`, reclamaciones por (incidencia, pedido), avisos por (incidencia, pedido, versión del plan), reembolsos por incidencia y pedido, reservas activas por (incidencia, pedido).
- **Criterio.** H23: todos los recuentos igualan los máximos que fijan las specs `idempotency` e `incident-interpretation`.
- **Implementación.** `uv run eda-expexecutor run e23`, una ejecución por variante. (a) reenvía cada aviso con `Harness.resend_notice` (en orden) o `Harness.resend_notice_concurrently` (a la vez, un hilo y una conexión por envío). (b) y (c) usan oráculos de replan propios: `RerequestOracle` pide otra vez `file_claim` y `notify_customer` ya completados con `step_id` nuevos (la reserva tarda 2 s en responder para que ya estén completados); `RefundAgainOracle` mantiene tras el reembolso la reserva fallida, con su `step_id` y su clave, así que vuelve a fallar y el replan siguiente propone `issue_refund` otra vez. Los dos proponen planes que el replanner acepta (`check_revision`): no repiten pasos completados ni cambian el `step_id` de un paso abierto. (d) da a uno de cada cuatro pedidos un segundo aviso con otro `carrier_notice_id` y las mismas etiquetas, porque el oráculo indexa por pedido. Para que el sistema cumpla H23, el ejecutor de `issue_refund` devuelve el reembolso existente de la incidencia y el pedido, y el de `notify_customer` el aviso del mismo pedido en la misma versión del plan (migración 0025). Cada variante debe provocar su duplicado; si no, no cuenta como cumplida.

## 6. Limitaciones del arnés y orden de implantación

**Limitaciones.** El arnés actual es en proceso: sirve para duplicados, retrasos, fallos por probabilidad, caída de un consumidor y almacenes vacíos, y todo corre en un mismo proceso, de modo que no puede reproducir congelaciones de procesos, rebalanceos reales ni cortes de Kafka y PostgreSQL. E9, E10, E12 y E16 exigen un modo multiproceso (procesos de `scripts/run-agents.sh`, `docker stop/pause`, `SIGSTOP` o un proxy tipo toxiproxy). Las propuestas de esta guía se basan en las specs, `design.md` y el código del arnés, no en haber ejecutado los experimentos.

**Orden sugerido.**

1. En proceso y con poco trabajo nuevo: E21, E23, E17, E18 y E11, que reutilizan el arnés con extensiones pequeñas (reinicio de offsets, replan forzado, parada de proyectores, inyección de veneno, retraso por acción).
2. E22: requiere hilos con conexiones propias y un punto de caída nuevo en los ejecutores.
3. Con modo multiproceso: E12 y E16 (y después E9 y E10).
4. El resto (E8, E13, E14, E15, E19, E20) según el interés del trabajo.

Los ficheros de especificación de los experimentos (S) están en `specs-experimentos-resiliencia.md`, en formato OpenSpec (Purpose, Requirements, Scenarios con GIVEN/WHEN/THEN).
