# Expexecutor Specification

## Purpose

Ejecutar los experimentos de la PoC y documentar cada ejecución con un informe de resultados legible, generado sin LLM a partir de lo que la propia ejecución deja escrito. El agente `expexecutor` es determinista: no interpreta ni resume con un modelo; ordena, compara y cita.

## Requirements

### Requirement: Agente determinista

El `expexecutor` SHALL generar el informe de resultados solo con código, sin llamar a ningún LLM. Dada la misma carpeta de ejecución, MUST producir un informe idéntico byte a byte.

#### Scenario: Mismo informe para la misma ejecución

- **GIVEN** una carpeta de ejecución con su `report.json` y su `experiment.log`
- **WHEN** se genera el informe de resultados dos veces
- **THEN** los dos `results.md` son idénticos
- **AND** no se ha hecho ninguna llamada a un LLM

### Requirement: CLI del expexecutor

El `expexecutor` SHALL ofrecer un CLI, `uv run eda-expexecutor`, con tres subcomandos: `run` (ejecutar un experimento y generar su informe de resultados), `report` (generar el informe de resultados de una ejecución ya hecha) y `delete` (borrar el informe de resultados de una ejecución o todos los datos de los experimentos). Cada subcomando MUST terminar con código 0 si tiene éxito y distinto de 0 si falla, con un mensaje de error en español.

#### Scenario: Ayuda del CLI

- **GIVEN** el CLI instalado
- **WHEN** se ejecuta `uv run eda-expexecutor --help`
- **THEN** la ayuda lista los subcomandos `run`, `report` y `delete`

#### Scenario: Subcomando desconocido

- **GIVEN** el CLI instalado
- **WHEN** se ejecuta `uv run eda-expexecutor publish`
- **THEN** termina con un código distinto de 0 y un mensaje de error en español

### Requirement: Ejecución de un experimento

`eda-expexecutor run <exp>` SHALL ejecutar el experimento con `eda-experiment`, pasándole sin cambios las opciones de este (`--seed`, `--incidents`, `--llm`, `--out`…), y MUST guardar toda su salida (stdout y stderr) en `experiment.log`, dentro de la carpeta de la ejecución (`<out>/<exp>-<fecha>/`), junto a `report.json` y `report.md`. Al terminar el experimento, SHALL generar el informe de resultados de esa ejecución.

#### Scenario: Ejecución completa

- **GIVEN** el experimento E1 y la semilla 42
- **WHEN** se ejecuta `uv run eda-expexecutor run e1 --seed 42`
- **THEN** la carpeta de la ejecución contiene `report.json`, `report.md`, `experiment.log` y `results.md`
- **AND** `report.json` registra la semilla 42

#### Scenario: Experimento que falla

- **GIVEN** un experimento que termina con un código distinto de 0
- **WHEN** se ejecuta con `eda-expexecutor run`
- **THEN** `experiment.log` guarda su salida completa
- **AND** se genera `results.md` con el resultado obtenido «ejecución fallida» y, como evidencia, las últimas líneas del log
- **AND** el CLI termina con un código distinto de 0

#### Scenario: Experimento desconocido

- **GIVEN** un nombre que no es ningún experimento de `specs/experiments`
- **WHEN** se ejecuta `uv run eda-expexecutor run e99`
- **THEN** termina con un código distinto de 0 sin ejecutar nada ni crear ninguna carpeta

### Requirement: Fichero de configuración de los experimentos

Los parámetros de ejecución de los experimentos que los necesitan SHALL poder fijarse en un fichero de configuración Python, `experiments/config.py`, que define un diccionario `EXPERIMENTS` con una entrada por experimento y sus parámetros (los de `eda-experiment`: `seed`, `incidents`, `llm`, `repetitions`, `threshold`, `temperature`); por ejemplo, `EXPERIMENTS = {"e5": {"llm": "ollama", "repetitions": 3, "threshold": 0.8}}`. `eda-expexecutor run <exp>` SHALL leer la entrada de ese experimento y pasarla a `eda-experiment`; con `--config <fichero>` se usa otro fichero. Un parámetro dado en la línea de comandos MUST ganar al del fichero, y el del fichero, al valor por defecto de `eda-experiment`. Un experimento sin entrada SHALL ejecutarse con los valores por defecto. El expexecutor MUST validar el fichero antes de ejecutar nada: un experimento desconocido, un parámetro desconocido, un parámetro que ese experimento no acepta o un valor de tipo incorrecto terminan con error. El informe de resultados MUST listar en «Evidencias» los parámetros aplicados y el origen de cada uno (línea de comandos, fichero de configuración o valor por defecto).

#### Scenario: Parámetros desde el fichero

- **GIVEN** `experiments/config.py` con `EXPERIMENTS = {"e5": {"llm": "oracle", "repetitions": 3}}`
- **WHEN** se ejecuta `uv run eda-expexecutor run e5`
- **THEN** E5 se ejecuta con `--llm oracle --repetitions 3`
- **AND** «Evidencias» indica que esos dos parámetros vienen del fichero de configuración

#### Scenario: La línea de comandos gana al fichero

- **GIVEN** `experiments/config.py` con `EXPERIMENTS = {"e1": {"seed": 7}}`
- **WHEN** se ejecuta `uv run eda-expexecutor run e1 --seed 42`
- **THEN** E1 se ejecuta con la semilla 42
- **AND** «Evidencias» indica que la semilla viene de la línea de comandos

#### Scenario: Experimento sin entrada en el fichero

- **GIVEN** `experiments/config.py` sin entrada para E4
- **WHEN** se ejecuta `uv run eda-expexecutor run e4`
- **THEN** E4 se ejecuta con los valores por defecto de `eda-experiment`

#### Scenario: Fichero de configuración alternativo

- **GIVEN** un fichero `mi-config.py` con `EXPERIMENTS = {"e2": {"incidents": 5}}`
- **WHEN** se ejecuta `uv run eda-expexecutor run e2 --config mi-config.py`
- **THEN** E2 se ejecuta con 5 incidencias

#### Scenario: Parámetro no válido para el experimento

- **GIVEN** `experiments/config.py` con `EXPERIMENTS = {"e1": {"threshold": 0.9}}`
- **WHEN** se ejecuta `uv run eda-expexecutor run e1`
- **THEN** termina con un código distinto de 0 y un mensaje que nombra el parámetro y el experimento
- **AND** no se ejecuta el experimento ni se crea ninguna carpeta

#### Scenario: Valor de tipo incorrecto

- **GIVEN** `experiments/config.py` con `EXPERIMENTS = {"e5": {"repetitions": "tres"}}`
- **WHEN** se ejecuta `uv run eda-expexecutor run e5`
- **THEN** termina con un código distinto de 0 sin ejecutar el experimento

### Requirement: Generación del informe de una ejecución existente

`eda-expexecutor report <carpeta>` SHALL generar (o regenerar) `results.md` de una ejecución ya hecha a partir de su `report.json` y de su `experiment.log`, sin volver a ejecutar el experimento. Si la carpeta no tiene `report.json`, MUST terminar con error y no escribir nada. Si no tiene `experiment.log`, SHALL generar el informe solo con `report.json` e indicarlo en la sección de evidencias.

#### Scenario: Regenerar un informe

- **GIVEN** una ejecución con `report.json`, `experiment.log` y un `results.md` borrado
- **WHEN** se ejecuta `uv run eda-expexecutor report <carpeta>`
- **THEN** se vuelve a crear `results.md` sin ejecutar el experimento

#### Scenario: Ejecución anterior sin log

- **GIVEN** una ejecución anterior al expexecutor, con `report.json` y sin `experiment.log`
- **WHEN** se genera su informe de resultados
- **THEN** `results.md` se genera con las evidencias de `report.json`
- **AND** la sección de evidencias indica que no hay log de la ejecución

#### Scenario: Carpeta sin informe del experimento

- **GIVEN** una carpeta sin `report.json`
- **WHEN** se ejecuta `uv run eda-expexecutor report <carpeta>`
- **THEN** termina con un código distinto de 0 y no se crea `results.md`

### Requirement: Formato del informe de resultados

El informe de resultados SHALL ser un fichero Markdown, `results.md`, en la carpeta de la ejecución, con estas secciones en este orden: `# Resultados de <exp>`; `## Propósito`, que introduce qué mide el experimento y su hipótesis; `## Resultado`, con el resultado esperado, el obtenido y si coinciden; y `## Evidencias`. El propósito y el resultado esperado MUST salir de la definición del experimento en `specs/experiments` (hipótesis y umbrales), recogida en un catálogo del expexecutor; el resultado obtenido, de `report.json`.

#### Scenario: Secciones del informe

- **GIVEN** una ejecución de E1 con la hipótesis H1 cumplida
- **WHEN** se genera su informe de resultados
- **THEN** `results.md` tiene, en este orden, el título «Resultados de E1» y las secciones «Propósito», «Resultado» y «Evidencias»
- **AND** la sección «Resultado» muestra el resultado esperado de H1, el obtenido y que coinciden

#### Scenario: Hipótesis no cumplida

- **GIVEN** una ejecución cuyo `report.json` marca su hipótesis como no cumplida
- **WHEN** se genera su informe de resultados
- **THEN** la sección «Resultado» indica que el resultado obtenido no coincide con el esperado y en qué métrica difiere

### Requirement: Evidencias

La sección de evidencias SHALL contener solo datos tomados de los informes de la ejecución (`report.json` y `experiment.log`): la semilla, la configuración, el modelo, la versión de los prompts, las métricas en las que se basa el resultado y las líneas del log que lo respaldan (avisos y errores incluidos). Cada evidencia MUST citar su origen: el fichero y la clave de `report.json` o el número de línea de `experiment.log`. El expexecutor MUST NOT incluir datos que no estén en esos ficheros.

#### Scenario: Evidencias con su origen

- **GIVEN** una ejecución de E1 con métricas de efectos duplicados en `report.json`
- **WHEN** se genera su informe de resultados
- **THEN** cada evidencia de la sección «Evidencias» indica de qué fichero sale y de qué clave o línea

#### Scenario: Errores del log como evidencia

- **GIVEN** un `experiment.log` con una línea de error
- **WHEN** se genera el informe de resultados
- **THEN** esa línea aparece en «Evidencias» con su número de línea

### Requirement: Borrado de informes de resultados

`eda-expexecutor delete <carpeta>` SHALL borrar el informe de resultados (`results.md`) de esa ejecución y MUST conservar su `report.json`, `report.md` y `experiment.log`, para que el informe se pueda regenerar con `report`. Si la carpeta no existe o no tiene `results.md`, MUST terminar con error sin borrar nada. `eda-expexecutor delete --all` SHALL borrar todos los datos de todos los experimentos: todas las carpetas de ejecución de la carpeta de resultados (`--out`, por defecto `experiments/results`), con su `report.json`, `report.md`, `experiment.log` y `results.md`. MUST NOT borrar nada fuera de la carpeta de resultados (ni el fichero de configuración `experiments/config.py`). `delete` sin carpeta ni `--all` MUST terminar con error.

#### Scenario: Borrar un informe

- **GIVEN** dos ejecuciones con su `results.md`
- **WHEN** se ejecuta `uv run eda-expexecutor delete <carpeta de la primera>`
- **THEN** solo desaparece el `results.md` de la primera
- **AND** su `report.json`, `report.md` y `experiment.log` siguen ahí

#### Scenario: Borrar todos los datos de los experimentos

- **GIVEN** tres ejecuciones de experimentos distintos, cada una con `report.json`, `report.md`, `experiment.log` y `results.md`
- **WHEN** se ejecuta `uv run eda-expexecutor delete --all`
- **THEN** no queda ninguna carpeta de ejecución en la carpeta de resultados
- **AND** `experiments/config.py` sigue ahí

#### Scenario: Borrar un informe que no existe

- **GIVEN** una carpeta sin `results.md`
- **WHEN** se ejecuta `uv run eda-expexecutor delete <carpeta>`
- **THEN** termina con un código distinto de 0 y no se borra nada

#### Scenario: Borrado sin destino

- **GIVEN** el CLI instalado
- **WHEN** se ejecuta `uv run eda-expexecutor delete` sin carpeta ni `--all`
- **THEN** termina con un código distinto de 0 y no se borra nada
