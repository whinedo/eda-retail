# Synthetic data Specification

## Purpose

Disponer de avisos de transportistas y de fallos inyectables que sean repetibles, para que los experimentos se puedan ejecutar y comparar sin datos reales.

## Requirements

### Requirement: Generador de avisos

El generador SHALL producir avisos con las variantes `delay`, `loss`, `damage`, `wrong_address`, `strike` y `unknown`, en texto libre y en formato de API, y con una etiqueta de la `cause` y del plan esperado.

#### Scenario: Conjunto de evaluación

- **GIVEN** una semilla y un tamaño de 50 avisos
- **WHEN** se ejecuta el generador
- **THEN** produce 50 avisos con su causa y su plan esperado
- **AND** incluye al menos cinco de cada variante

### Requirement: Repetibilidad

Con la misma semilla, el generador MUST producir exactamente el mismo conjunto.

#### Scenario: Dos ejecuciones con la misma semilla

- **GIVEN** la semilla 42
- **WHEN** se ejecuta el generador dos veces
- **THEN** los dos conjuntos son idénticos

### Requirement: Datos simulados de negocio

El sistema SHALL simular pedidos, existencias por almacén, transportistas, correo y pagos, sin conexión con sistemas reales. Los pedidos y el stock de ejemplo son de `inventory`, que los carga con el comando `SeedSampleData` y publica un `OrderRegistered` por cada pedido nuevo.

#### Scenario: Datos de ejemplo por comando

- **GIVEN** `inventory` sin el pedido P-1001
- **WHEN** atiende un `SeedSampleData` que lo incluye
- **THEN** escribe el pedido en `inventory.orders` y `OrderRegistered` de P-1001 en `inventory.outbox`
- **AND** otro `SeedSampleData` con los mismos datos no escribe ningún `OrderRegistered`

#### Scenario: Reserva de stock simulada

- **GIVEN** un almacén simulado con 3 unidades del artículo A1
- **WHEN** se reservan 4 unidades
- **THEN** la reserva falla con `out_of_stock`

### Requirement: Inyección de fallos

El sistema SHALL permitir inyectar fallos configurables: duplicar eventos (de 1 a N veces), retrasar su entrega, hacer fallar un tipo de paso con una probabilidad dada, caer un consumidor en un instante dado y dejar sin stock un almacén.

#### Scenario: Duplicación de eventos

- **GIVEN** la inyección configurada para entregar cada evento 3 veces
- **WHEN** se ejecuta una incidencia
- **THEN** cada consumidor recibe cada evento 3 veces

### Requirement: Fallos reproducibles

La inyección de fallos MUST ser determinista dada una semilla.

#### Scenario: Misma semilla, mismos fallos

- **GIVEN** una semilla y una probabilidad de fallo del 20 % en `reserve_stock`
- **WHEN** se ejecuta dos veces el mismo conjunto
- **THEN** los mismos pasos fallan en las dos ejecuciones
