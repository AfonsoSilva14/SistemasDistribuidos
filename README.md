# Sistema de Monitorização Urbana — One Health (TP2)

Sistema distribuído de monitorização ambiental urbana. Evolução do TP1
com **Pub/Sub (RabbitMQ)**, **RPC**, persistência em **SQLite** e uma
**interface de visualização e análise** por linha de comandos.

## Arquitetura

```
                 RabbitMQ (Pub/Sub, topic)
SENSOR ──publica──►  sensores_exchange  ──subscreve──►  GATEWAY
                                                          │
                                  RPC (HTTP/JSON) ────────►│ PreProcessingService (C#, :7001)
                                                          │   uniformiza unidades/escalas
                                                          ▼
                                                       SERVIDOR  (TCP :5000)
                                                          │
                                  RPC (HTTP/JSON) ────────►│ AnalysisService (C#, :7002)
                                                          │   estatística + risco saúde
                                                          ▼
                                                      servidor.db (SQLite)
                                                          ▲
                                              CONSOLA (Interface CLI) ─ consulta + pede análises
```

## Componentes

| Componente | Tecnologia | Papel |
|------------|-----------|-------|
| **Sensor** | C# / RabbitMQ.Client | Publica medições e heartbeats em tópicos `zona.tipo` |
| **Gateway** | C# / RabbitMQ + SQLite | Subscreve tópicos da sua zona, valida, invoca pré-processamento (RPC), agrega, reencaminha ao Servidor |
| **PreProcessingService** | **C#** (ASP.NET minimal API) | RPC — uniformização: normaliza tipos, **converte escalas/unidades** (F→C, K→C, Pa→hPa, mg/m³→µg/m³) |
| **Servidor** | C# / SQLite | Persiste medições, invoca análise (RPC) e **persiste resultados das análises** |
| **AnalysisService** | **C#** (ASP.NET minimal API) | RPC — classificação de risco + análise estatística (média, mín, máx, desvio-padrão, tendência) |
| **Consola** | C# / SQLite | Interface CLI: consultar medições, **pedir análises parametrizadas**, ver histórico |

> O pré-processamento com **conversão de escalas/unidades** e a **análise
> estatística rica** (com deteção de tendência e limiares OMS) são fatores
> de valorização.

## Pré-requisitos

- .NET 8 SDK
- RabbitMQ a correr em `localhost:5672` (ex.: `docker run -d --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:3-management`)

## Ordem de arranque

Abrir um terminal por componente (ou usar *Multiple startup projects* no Visual Studio):

```bash
# 1. Serviço de análise (C#)
cd SistemaMonitorizacao/AnalysisService && dotnet run

# 2. Serviço de pré-processamento (C#)
cd SistemaMonitorizacao/PreProcessingService && dotnet run

# 3. Servidor
cd SistemaMonitorizacao/Servidor && dotnet run

# 4. Gateway (precisa do RabbitMQ e do Servidor já a correr)
cd SistemaMonitorizacao/Gateway && dotnet run

# 5. Sensor  (args opcionais: <sensorId> <zona>)
cd SistemaMonitorizacao/Sensor && dotnet run -- S102 ZONA_ESCOLAR

# 6. Interface CLI (a qualquer momento, lê a BD do Servidor)
cd SistemaMonitorizacao/Consola && dotnet run
```

## Base de dados (servidor.db)

| Tabela | Conteúdo |
|--------|----------|
| `MedicoesServidor` | Todas as medições recebidas |
| `Analises` | Resultados das análises (tempo real + pedidas pela Consola) |

## Endpoints RPC do AnalysisService (porta 7002)

| Método | Endpoint | Quem invoca | Descrição |
|--------|----------|-------------|-----------|
| POST | `/analyze` | Servidor (tempo real) | `{Tipo,Valor}` → classificação de risco |
| POST | `/analyze-batch` | Consola | `{Tipo,Valores:[...]}` → média, mín, máx, desvio-padrão, tendência, risco |
## Funcionalidades extra implementadas

- Gateway com configuracao multi-sensor em `SistemaMonitorizacao/Gateway/sensores_config.csv`.
- Suporte multi-zona: o Gateway subscreve automaticamente as zonas dos sensores configurados.
- Modo automatico no Sensor: `dotnet run -- S103 ZONA_ESCOLAR --auto --interval 5`.
- Alertas persistidos no Servidor quando a analise devolve resultado diferente de `NORMAL`.
- Consola com dashboard operacional, consulta/resolucao de alertas e exportacao para CSV/JSON.
