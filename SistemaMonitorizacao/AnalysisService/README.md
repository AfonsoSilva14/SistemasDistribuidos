# AnalysisService (Python)

Serviço de Análise e Previsão do TP2, implementado em **Python** (linguagem
diferente do resto do sistema, em C#) como fator de valorização.

É invocado remotamente (RPC sobre HTTP/JSON) pelo **Servidor** e pela
**Interface CLI**.

## Como correr

```bash
cd SistemaMonitorizacao/AnalysisService
python3 analysis_service.py
```

Escuta em `http://localhost:7002`. Não requer instalação de dependências
(usa apenas a biblioteca-padrão do Python).

## Endpoints

| Método | Endpoint | Quem invoca | Descrição |
|--------|----------|-------------|-----------|
| POST | `/analyze` | Servidor (tempo real) | Classifica o risco de uma medição individual `{Tipo, Valor}` |
| POST | `/analyze-batch` | Interface CLI | Análise estatística de um conjunto `{Tipo, Valores:[...]}` → média, min, máx, desvio-padrão, tendência e risco |

### Exemplo `/analyze-batch`

Pedido:
```json
{ "Tipo": "PM25", "Valores": [12.0, 18.5, 30.2, 41.0] }
```

Resposta:
```json
{
  "Tipo": "PM25", "Quantidade": 4, "Media": 25.43,
  "Minimo": 12.0, "Maximo": 41.0, "DesvioPadrao": 10.83,
  "Tendencia": "A_SUBIR", "Resultado": "ALERTA"
}
```
