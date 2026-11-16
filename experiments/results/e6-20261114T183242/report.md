# E6

- Semilla: 42
- Modelo: qwen2.5:7b-instruct
- Versión de los prompts: `4cf0d92f0362`

## Hipótesis


## Métricas

```json
{
  "no_faults": {
    "events": {
      "latency_ms": {
        "mean": 1751865.3914333333,
        "median": 1939652.2275,
        "p95": 2270748.781
      },
      "duplicates": 0,
      "closed": {
        "mean": 50.0,
        "min": 50,
        "max": 50
      },
      "resolved": {
        "mean": 50.0,
        "min": 50,
        "max": 50
      },
      "escalated": {
        "mean": 0.0,
        "min": 0,
        "max": 0
      },
      "refunded": {
        "mean": 0.0,
        "min": 0,
        "max": 0
      },
      "llm_calls": {
        "mean": 178.33333333333334,
        "min": 175,
        "max": 181
      },
      "tokens_in": {
        "mean": 54517.333333333336,
        "min": 52588,
        "max": 55928
      },
      "tokens_out": {
        "mean": 19410.333333333332,
        "min": 18541,
        "max": 19864
      },
      "cost": {
        "mean": 0.0,
        "min": 0.0,
        "max": 0.0
      }
    },
    "sync": {
      "latency_ms": {
        "mean": 38098.434894711056,
        "median": 40569.85052100208,
        "p95": 56580.195457994705
      },
      "duplicates": 0,
      "closed": {
        "mean": 50.0,
        "min": 50,
        "max": 50
      },
      "resolved": {
        "mean": 50.0,
        "min": 50,
        "max": 50
      },
      "escalated": {
        "mean": 0.0,
        "min": 0,
        "max": 0
      },
      "refunded": {
        "mean": 0.0,
        "min": 0,
        "max": 0
      },
      "llm_calls": {
        "mean": 178.33333333333334,
        "min": 175,
        "max": 181
      },
      "tokens_in": {
        "mean": 54517.333333333336,
        "min": 52588,
        "max": 55928
      },
      "tokens_out": {
        "mean": 19419.666666666668,
        "min": 18536,
        "max": 19867
      },
      "cost": {
        "mean": 0.0,
        "min": 0.0,
        "max": 0.0
      }
    }
  },
  "faults": {
    "events": {
      "latency_ms": {
        "mean": 1909641.17448,
        "median": 1885452.1055,
        "p95": 2234674.661
      },
      "duplicates": 0,
      "closed": {
        "mean": 50.0,
        "min": 50,
        "max": 50
      },
      "resolved": {
        "mean": 43.666666666666664,
        "min": 42,
        "max": 45
      },
      "escalated": {
        "mean": 6.333333333333333,
        "min": 5,
        "max": 8
      },
      "refunded": {
        "mean": 2.3333333333333335,
        "min": 1,
        "max": 3
      },
      "llm_calls": {
        "mean": 200.33333333333334,
        "min": 193,
        "max": 206
      },
      "tokens_in": {
        "mean": 65288.333333333336,
        "min": 61344,
        "max": 68056
      },
      "tokens_out": {
        "mean": 21709.666666666668,
        "min": 19985,
        "max": 22812
      },
      "cost": {
        "mean": 0.0,
        "min": 0.0,
        "max": 0.0
      }
    },
    "sync": {
      "latency_ms": {
        "mean": 42252.40718222058,
        "median": 43850.5416455082,
        "p95": 68564.19037500746
      },
      "duplicates": 0,
      "closed": {
        "mean": 50.0,
        "min": 50,
        "max": 50
      },
      "resolved": {
        "mean": 47.666666666666664,
        "min": 47,
        "max": 48
      },
      "escalated": {
        "mean": 2.3333333333333335,
        "min": 2,
        "max": 3
      },
      "refunded": {
        "mean": 6.333333333333333,
        "min": 6,
        "max": 7
      },
      "llm_calls": {
        "mean": 194.0,
        "min": 187,
        "max": 198
      },
      "tokens_in": {
        "mean": 61480.666666666664,
        "min": 58292,
        "max": 63316
      },
      "tokens_out": {
        "mean": 20951.666666666668,
        "min": 19522,
        "max": 21951
      },
      "cost": {
        "mean": 0.0,
        "min": 0.0,
        "max": 0.0
      }
    }
  }
}
```

## Configuración

```json
{
  "incidents": 50,
  "repetitions": 3,
  "scenarios": [
    "no_faults",
    "faults"
  ],
  "temperature": 0.7,
  "faults": {
    "duplicates": 2,
    "fail_probability": {
      "reserve_stock": 0.3
    }
  },
  "prices": {
    "in_per_1k": "0",
    "out_per_1k": "0"
  }
}
```

## Ejecuciones

```json
[
  {
    "repetition": 1,
    "scenario": "no_faults",
    "llm_seed": 43,
    "temperature": 0.7,
    "version": "events",
    "closed": 50,
    "resolved": 50,
    "escalated": 0,
    "refunded": 0,
    "duplicates": 0,
    "llm_calls": 175,
    "tokens_in": 52588,
    "tokens_out": 18541,
    "cost": 0.0,
    "elapsed_seconds": 1456.8649322499987
  },
  {
    "repetition": 1,
    "scenario": "no_faults",
    "llm_seed": 43,
    "temperature": 0.7,
    "version": "sync",
    "closed": 50,
    "resolved": 50,
    "escalated": 0,
    "refunded": 0,
    "duplicates": 0,
    "llm_calls": 175,
    "tokens_in": 52588,
    "tokens_out": 18536,
    "cost": 0.0,
    "elapsed_seconds": 1825.4262920839974
  },
  {
    "repetition": 1,
    "scenario": "faults",
    "llm_seed": 43,
    "temperature": 0.7,
    "version": "events",
    "closed": 50,
    "resolved": 45,
    "escalated": 5,
    "refunded": 3,
    "duplicates": 0,
    "llm_calls": 193,
    "tokens_in": 61344,
    "tokens_out": 19985,
    "cost": 0.0,
    "elapsed_seconds": 1886.4645364169992
  },
  {
    "repetition": 1,
    "scenario": "faults",
    "llm_seed": 43,
    "temperature": 0.7,
    "version": "sync",
    "closed": 50,
    "resolved": 48,
    "escalated": 2,
    "refunded": 6,
    "duplicates": 0,
    "llm_calls": 187,
    "tokens_in": 58292,
    "tokens_out": 19522,
    "cost": 0.0,
    "elapsed_seconds": 1939.8836962920032
  },
  {
    "repetition": 2,
    "scenario": "no_faults",
    "llm_seed": 44,
    "temperature": 0.7,
    "version": "events",
    "closed": 50,
    "resolved": 50,
    "escalated": 0,
    "refunded": 0,
    "duplicates": 0,
    "llm_calls": 181,
    "tokens_in": 55928,
    "tokens_out": 19826,
    "cost": 0.0,
    "elapsed_seconds": 1941.9364147919987
  },
  {
    "repetition": 2,
    "scenario": "no_faults",
    "llm_seed": 44,
    "temperature": 0.7,
    "version": "sync",
    "closed": 50,
    "resolved": 50,
    "escalated": 0,
    "refunded": 0,
    "duplicates": 0,
    "llm_calls": 181,
    "tokens_in": 55928,
    "tokens_out": 19867,
    "cost": 0.0,
    "elapsed_seconds": 1816.773308874981
  },
  {
    "repetition": 2,
    "scenario": "faults",
    "llm_seed": 44,
    "temperature": 0.7,
    "version": "events",
    "closed": 50,
    "resolved": 44,
    "escalated": 6,
    "refunded": 3,
    "duplicates": 0,
    "llm_calls": 202,
    "tokens_in": 66465,
    "tokens_out": 22332,
    "cost": 0.0,
    "elapsed_seconds": 2229.3814997920126
  },
  {
    "repetition": 2,
    "scenario": "faults",
    "llm_seed": 44,
    "temperature": 0.7,
    "version": "sync",
    "closed": 50,
    "resolved": 48,
    "escalated": 2,
    "refunded": 7,
    "duplicates": 0,
    "llm_calls": 197,
    "tokens_in": 62834,
    "tokens_out": 21382,
    "cost": 0.0,
    "elapsed_seconds": 2094.614301249996
  },
  {
    "repetition": 3,
    "scenario": "no_faults",
    "llm_seed": 45,
    "temperature": 0.7,
    "version": "events",
    "closed": 50,
    "resolved": 50,
    "escalated": 0,
    "refunded": 0,
    "duplicates": 0,
    "llm_calls": 179,
    "tokens_in": 55036,
    "tokens_out": 19864,
    "cost": 0.0,
    "elapsed_seconds": 2271.620374082995
  },
  {
    "repetition": 3,
    "scenario": "no_faults",
    "llm_seed": 45,
    "temperature": 0.7,
    "version": "sync",
    "closed": 50,
    "resolved": 50,
    "escalated": 0,
    "refunded": 0,
    "duplicates": 0,
    "llm_calls": 179,
    "tokens_in": 55036,
    "tokens_out": 19856,
    "cost": 0.0,
    "elapsed_seconds": 2072.840124999988
  },
  {
    "repetition": 3,
    "scenario": "faults",
    "llm_seed": 45,
    "temperature": 0.7,
    "version": "events",
    "closed": 50,
    "resolved": 42,
    "escalated": 8,
    "refunded": 1,
    "duplicates": 0,
    "llm_calls": 206,
    "tokens_in": 68056,
    "tokens_out": 22812,
    "cost": 0.0,
    "elapsed_seconds": 2235.438432875002
  },
  {
    "repetition": 3,
    "scenario": "faults",
    "llm_seed": 45,
    "temperature": 0.7,
    "version": "sync",
    "closed": 50,
    "resolved": 47,
    "escalated": 3,
    "refunded": 6,
    "duplicates": 0,
    "llm_calls": 198,
    "tokens_in": 63316,
    "tokens_out": 21951,
    "cost": 0.0,
    "elapsed_seconds": 2303.8702389590035
  }
]
```
