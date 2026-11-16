# Experiments Specification

## Purpose

Definir las hipótesis, los experimentos y las métricas de la PoC académica. La arquitectura orientada a eventos se compara con una base síncrona equivalente. Los umbrales indicados son valores iniciales ajustables.

## Requirements

### Requirement: Base síncrona de referencia

El sistema SHALL incluir una implementación síncrona con la misma lógica de negocio, los mismos agentes con LLM y los mismos ejecutores, pero con llamadas directas en lugar de bus, log y vistas. MUST usar los mismos datos sintéticos y los mismos fallos inyectados.

#### Scenario: Misma entrada en ambas versiones

- **GIVEN** un conjunto de 50 avisos con una semilla
- **WHEN** se ejecuta en la versión de eventos y en la síncrona
- **THEN** ambas reciben exactamente los mismos avisos y fallos

### Requirement: Informe reproducible

Cada experimento SHALL poder ejecutarse con un único comando y generar un informe con la semilla, la configuración, la versión de los prompts y de los modelos y las métricas. MUST ser posible repetirlo con el mismo resultado de los componentes deterministas.

#### Scenario: Ejecución de un experimento

- **GIVEN** la configuración del experimento E1
- **WHEN** se ejecuta
- **THEN** se genera un informe con sus métricas y la configuración usada

### Requirement: E1 Duplicados en el bus (hipótesis H1)

El experimento E1 SHALL entregar cada evento a cada consumidor de Kafka y cada comando REST que invoca la saga 2 y 3 veces (el mismo `command_id`, como un reintento HTTP de entrega al menos una vez) y comparar el sistema con y sin Idempotency key. Sin idempotencia, los ejecutores e `inventory` pierden su inbox de comandos, `effect_keys` y la clave en los servicios simulados. Hipótesis H1: con idempotencia el número de efectos duplicados es 0 en todas las ejecuciones; sin ella, mayor que 0.

#### Scenario: Con idempotencia

- **GIVEN** 50 incidencias y cada evento y cada comando entregados 3 veces
- **WHEN** se ejecuta E1 con las claves activas
- **THEN** el informe indica 0 avisos, reclamaciones, reservas y reembolsos duplicados

#### Scenario: Sin idempotencia

- **GIVEN** las mismas 50 incidencias y las claves desactivadas
- **WHEN** se ejecuta E1
- **THEN** el informe cuantifica los efectos duplicados por tipo

### Requirement: E2 Caída de un ejecutor (hipótesis H2)

El experimento E2 SHALL matar un ejecutor a mitad de un plan y reiniciarlo: cae después de confirmar su transacción y antes de responder al `ExecuteStep`, y la saga lo vuelve a invocar con el mismo `command_id`. Hipótesis H2: el 100 % de los planes se completa sin repetir ningún paso ya completado.

#### Scenario: Caída a mitad del plan

- **GIVEN** 50 incidencias y un ejecutor que cae tras completar la mitad de sus pasos
- **WHEN** se reinicia
- **THEN** todas las incidencias terminan en `IncidentResolved` o `IncidentEscalated`
- **AND** el informe indica 0 pasos repetidos

### Requirement: E3 Fallo de un paso y replanificación (hipótesis H3)

El experimento E3 SHALL dejar sin stock el almacén en una parte de las incidencias. Hipótesis H3: en el 100 % de los casos el replanner genera un plan que no repite pasos completados y que respeta el límite de reembolso.

#### Scenario: Almacén sin existencias

- **GIVEN** 30 incidencias de pérdida con el almacén sin stock
- **WHEN** se ejecuta E3
- **THEN** cada incidencia termina con un reembolso o con un escalado
- **AND** ningún `notify_customer` ni `file_claim` completado se repite

### Requirement: E4 Reconstrucción de vistas (hipótesis H4)

El experimento E4 SHALL borrar cada vista y reconstruirla volviendo a consumir el topic `eda.events` desde el principio. Hipótesis H4: la huella de cada vista reconstruida coincide con la original, y el tiempo de reconstrucción se registra.

#### Scenario: Reconstrucción completa

- **GIVEN** un log de 200 incidencias y las vistas pobladas
- **WHEN** se borran y se reproducen
- **THEN** la huella de cada vista coincide con la original
- **AND** el informe indica el tiempo por vista

### Requirement: E5 Calidad del plan (hipótesis H5)

El experimento E5 SHALL medir el acierto del planificador sobre un conjunto etiquetado de 30 a 50 incidencias, comparando el plan generado con el esperado. Hipótesis H5: el acierto en la selección de acciones es al menos del 80 % (valor inicial).

#### Scenario: Evaluación del planificador

- **GIVEN** un conjunto etiquetado de 40 incidencias
- **WHEN** se ejecuta E5
- **THEN** el informe indica el acierto global y por tipo de causa
- **AND** lista los planes que no coinciden

#### Scenario: Puerta de calidad

- **GIVEN** un acierto inferior al umbral configurado
- **WHEN** termina E5
- **THEN** el informe marca la hipótesis H5 como no cumplida

### Requirement: E6 Eventos frente a síncrono (hipótesis H6)

El experimento E6 SHALL comparar las dos versiones en latencia total por incidencia, tokens, coste, comportamiento ante fallos y número de incidencias resueltas y escaladas, con las escaladas contadas por motivo. H6 no fija un umbral: el objetivo es medir y describir las diferencias, con la media, la mediana y el percentil 95. E6 SHALL escalar los plazos de la saga con el número de incidencias, sin bajar de los valores por defecto, porque la base síncrona no tiene plazos y el LLM es una sola cola compartida; el informe SHALL decir qué plazos se usaron.

#### Scenario: Comparación sin fallos

- **GIVEN** 50 incidencias sin fallos inyectados
- **WHEN** se ejecutan en ambas versiones
- **THEN** el informe incluye la latencia, los tokens y el coste de cada versión
- **AND** el informe incluye las incidencias escaladas de cada versión, contadas por motivo

#### Scenario: Comparación con fallos

- **GIVEN** las mismas incidencias con fallos y duplicados inyectados
- **WHEN** se ejecutan en ambas versiones
- **THEN** el informe incluye las incidencias resueltas y los efectos duplicados de cada versión

### Requirement: E7 Auditabilidad (hipótesis H7)

El experimento E7 SHALL seleccionar incidencias al azar y reconstruir su explicación solo con el log y la traza. Hipótesis H7: para el 100 % de las seleccionadas se puede indicar qué agente propuso cada paso, con qué versión del plan y por qué.

#### Scenario: Explicación de una incidencia

- **GIVEN** 10 incidencias elegidas al azar
- **WHEN** se reconstruye su explicación con `audit_trail` y la traza
- **THEN** cada decisión tiene agente, versión del plan y motivo

### Requirement: Repetición para el componente no determinista

Los experimentos que dependen del LLM (E5, E6) MUST repetirse al menos tres veces y el informe SHALL mostrar la variación entre ejecuciones.

#### Scenario: Tres repeticiones de E5

- **GIVEN** E5 configurado con tres repeticiones
- **WHEN** termina
- **THEN** el informe muestra el acierto de cada repetición y su rango
