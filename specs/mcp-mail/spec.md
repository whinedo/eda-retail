# MCP mail server Specification

## Purpose

Enviar las notificaciones al cliente a través de un servidor de correo simulado que se usa con MCP (Model Context Protocol). Muestra el modo MCP: usar una herramienta que no razona y solo expone operaciones. Es parte de la segunda versión de la PoC (design.md §15).

## Requirements

### Requirement: Servidor de correo simulado

El sistema SHALL incluir un servidor MCP de correo, `mail-server`, como proceso aparte con su propio esquema `mail_server` en PostgreSQL. Ningún otro componente lee ni escribe ese esquema. El servidor MUST exponer una sola herramienta, `send_email`, con la entrada `customer_ref`, `subject`, `body` e `idempotency_key`, y la salida `message_id` y `accepted_at`. El servidor SHALL resolver la dirección de correo a partir de `customer_ref` en su propio directorio (`mail_server.customers`, sembrado con direcciones sintéticas) y guardar cada correo en su buzón simulado (`mail_server.mailbox`). La dirección MUST NOT salir del servidor: no aparece en la salida de la herramienta, ni en los eventos, ni en los logs de otros componentes.

#### Scenario: Correo enviado

- **GIVEN** el cliente `C-0042` en el directorio del servidor
- **WHEN** se invoca `send_email` con `customer_ref` `C-0042`, un asunto y un cuerpo
- **THEN** el buzón simulado guarda un correo dirigido a la dirección de `C-0042`
- **AND** la herramienta devuelve `message_id` y `accepted_at`, sin la dirección

#### Scenario: Cliente desconocido

- **GIVEN** un `customer_ref` que no está en el directorio
- **WHEN** se invoca `send_email`
- **THEN** la herramienta responde con un error MCP `unknown_customer`, no reintentable
- **AND** el buzón no cambia

#### Scenario: Envío repetido con la misma clave

- **GIVEN** un correo ya enviado con la `idempotency_key` K1
- **WHEN** se invoca `send_email` otra vez con K1
- **THEN** el servidor devuelve el mismo `message_id`
- **AND** el buzón sigue teniendo un solo correo con K1

### Requirement: El ejecutor de notify_customer es el cliente MCP

El ejecutor de `notify_customer` SHALL enviar el correo como cliente MCP del servidor de correo. Sustituye al servicio simulado en proceso. El cliente es código, no un agente: el LLM redactor solo produce el texto, ya validado (regla 1 de CLAUDE.md), y MUST NOT tener acceso a la herramienta. La llamada se hace en el worker del ejecutor, fuera de la transacción (spec `event-backbone`, «Invocación de comandos»), con la `idempotency_key` del paso. El resultado se guarda en `effect_keys` como hoy, así que un reintento del paso no envía dos correos. `CustomerNotified` lleva el `message_id` del servidor y ningún dato personal.

#### Scenario: Paso de notificación completado

- **GIVEN** un `ExecuteStep` de `notify_customer` aceptado y un borrador válido del redactor
- **WHEN** el worker del ejecutor lo procesa
- **THEN** invoca `send_email` en el servidor MCP con la `idempotency_key` del paso
- **AND** publica `CustomerNotified` con el `message_id` y `StepCompleted`

#### Scenario: Servidor de correo caído

- **GIVEN** el servidor MCP sin responder
- **WHEN** el worker invoca `send_email`
- **THEN** reintenta con espera exponencial hasta `STEP_MAX_ATTEMPTS`
- **AND** si se agotan los intentos, publica `StepFailed` con `retryable` verdadero

#### Scenario: Reintento tras enviar el correo

- **GIVEN** un correo ya enviado para el paso S1 y el worker caído antes de confirmar su transacción
- **WHEN** el trabajo se retoma
- **THEN** el servidor devuelve el mismo `message_id` para la `idempotency_key` de S1
- **AND** el buzón tiene un solo correo de S1

### Requirement: Guardrails del cliente MCP

El cliente MCP SHALL aplicar estos guardrails, porque el servidor de herramientas es un sistema externo:

- **Lista blanca de herramientas.** Al conectar, el cliente MUST comprobar que el servidor ofrece `send_email` con el esquema esperado. Solo invoca esa herramienta, aunque el servidor anuncie otras.
- **Validación de la entrada y de la salida** con modelos pydantic. Una salida que no cumple el esquema es un fallo no reintentable, y no se guarda ni se publica.
- **Límite de envíos por incidencia** (`MCP_MAX_EMAILS_PER_INCIDENT`, por defecto 3). Superarlo termina el paso en `StepFailed` no reintentable con `error` `mail_limit_exceeded`.
- **Sin datos personales en lo que vuelve.** Si la salida trae un campo que parece una dirección de correo, el cliente la descarta como inválida.

Cada fallo de un guardrail SHALL registrarse en el log con su motivo y su traza.

#### Scenario: Herramienta no permitida

- **GIVEN** un servidor MCP que anuncia `send_email` y `delete_mailbox`
- **WHEN** el cliente se conecta
- **THEN** solo registra `send_email` como herramienta invocable
- **AND** ninguna ruta del ejecutor puede invocar `delete_mailbox`

#### Scenario: Salida fuera de esquema

- **GIVEN** un servidor que devuelve `send_email` sin `message_id`
- **WHEN** el cliente valida la salida
- **THEN** el paso termina en `StepFailed` no reintentable con `error` `mcp_output_invalid`
- **AND** no se publica `CustomerNotified`

#### Scenario: Demasiados correos para una incidencia

- **GIVEN** una incidencia que ya recibió `MCP_MAX_EMAILS_PER_INCIDENT` correos
- **WHEN** llega otro paso de `notify_customer` de esa incidencia
- **THEN** el ejecutor no invoca `send_email`
- **AND** publica `StepFailed` no reintentable con `error` `mail_limit_exceeded`

#### Scenario: Dirección en la respuesta

- **GIVEN** un servidor que devuelve la dirección del destinatario en la salida
- **WHEN** el cliente valida la salida
- **THEN** la descarta como `mcp_output_invalid` y no la guarda ni la registra en el log
