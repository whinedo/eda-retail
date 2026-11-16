# E5

- Semilla: 42
- Modelo: qwen2.5:7b-instruct
- Versión de los prompts: `4cf0d92f0362`

## Hipótesis

- H5: no cumplida

## Métricas

```json
{
  "accuracy_mean": 0.75,
  "accuracy_range": [
    0.75,
    0.75
  ],
  "accuracy_per_repetition": [
    0.75,
    0.75,
    0.75
  ],
  "threshold": 0.8
}
```

## Configuración

```json
{
  "incidents": 40,
  "repetitions": 3,
  "threshold": 0.8
}
```

## Ejecuciones

```json
[
  {
    "repetition": 1,
    "accuracy": 0.75,
    "by_cause": {
      "damage": 1.0,
      "delay": 0.0,
      "loss": 1.0,
      "strike": 0.0,
      "unknown": 1.0,
      "wrong_address": 1.0
    },
    "llm_planned": 36,
    "code_planned": 4,
    "mismatches": [
      {
        "order_id": "P-S42N0003",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0007",
        "cause": "delay",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0009",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0012",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0015",
        "cause": "delay",
        "severity": "high",
        "expected": [
          "notify_customer",
          "file_claim"
        ],
        "proposed": [
          "notify_customer",
          "file_claim",
          "reserve_stock",
          "reissue_shipment"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0016",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0020",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0026",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0030",
        "cause": "delay",
        "severity": "high",
        "expected": [
          "notify_customer",
          "file_claim"
        ],
        "proposed": [
          "notify_customer",
          "file_claim",
          "reserve_stock",
          "reissue_shipment"
        ],
        "reason": null
      }
    ],
    "llm_calls": 36,
    "tokens_in": 20912,
    "tokens_out": 6547
  },
  {
    "repetition": 2,
    "accuracy": 0.75,
    "by_cause": {
      "damage": 1.0,
      "delay": 0.0,
      "loss": 1.0,
      "strike": 0.0,
      "unknown": 1.0,
      "wrong_address": 1.0
    },
    "llm_planned": 36,
    "code_planned": 4,
    "mismatches": [
      {
        "order_id": "P-S42N0003",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0007",
        "cause": "delay",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0009",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0012",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0015",
        "cause": "delay",
        "severity": "high",
        "expected": [
          "notify_customer",
          "file_claim"
        ],
        "proposed": [
          "notify_customer",
          "file_claim",
          "reserve_stock",
          "reissue_shipment"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0016",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0020",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0026",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0030",
        "cause": "delay",
        "severity": "high",
        "expected": [
          "notify_customer",
          "file_claim"
        ],
        "proposed": [
          "notify_customer",
          "file_claim",
          "reserve_stock",
          "reissue_shipment"
        ],
        "reason": null
      }
    ],
    "llm_calls": 36,
    "tokens_in": 20912,
    "tokens_out": 6547
  },
  {
    "repetition": 3,
    "accuracy": 0.75,
    "by_cause": {
      "damage": 1.0,
      "delay": 0.0,
      "loss": 1.0,
      "strike": 0.0,
      "unknown": 1.0,
      "wrong_address": 1.0
    },
    "llm_planned": 36,
    "code_planned": 4,
    "mismatches": [
      {
        "order_id": "P-S42N0003",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0007",
        "cause": "delay",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0009",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0012",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0015",
        "cause": "delay",
        "severity": "high",
        "expected": [
          "notify_customer",
          "file_claim"
        ],
        "proposed": [
          "notify_customer",
          "file_claim",
          "reserve_stock",
          "reissue_shipment"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0016",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0020",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0026",
        "cause": "strike",
        "severity": "medium",
        "expected": [
          "notify_customer"
        ],
        "proposed": [
          "notify_customer",
          "file_claim"
        ],
        "reason": null
      },
      {
        "order_id": "P-S42N0030",
        "cause": "delay",
        "severity": "high",
        "expected": [
          "notify_customer",
          "file_claim"
        ],
        "proposed": [
          "notify_customer",
          "file_claim",
          "reserve_stock",
          "reissue_shipment"
        ],
        "reason": null
      }
    ],
    "llm_calls": 36,
    "tokens_in": 20912,
    "tokens_out": 6547
  }
]
```
