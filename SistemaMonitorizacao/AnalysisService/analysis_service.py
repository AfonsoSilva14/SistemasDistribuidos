#!/usr/bin/env python3
"""
AnalysisService — Serviço de Análise e Previsão (RPC via HTTP/JSON)
====================================================================
Componente do TP2 implementado em Python (linguagem diferente do resto
do sistema, em C#) como fator de valorizacao.

Invocado remotamente pelo SERVIDOR (e pela Interface CLI) para:
  - Classificar risco de uma medicao individual          -> POST /analyze
  - Analise estatistica de um conjunto de medicoes        -> POST /analyze-batch

Sem dependencias externas: usa apenas a biblioteca-padrao do Python
(http.server, json, statistics), portanto nao requer `pip install`.

Porta: 7002  (mesma do antigo servico C#, mantendo compatibilidade)
"""

import json
import statistics
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = 7002

# Limiares de risco para a saude publica (referencias OMS / indices AQI)
LIMIARES = {
    "PM25":  {"alerta": 25.0, "elevado": 35.0, "unidade": "ug/m3"},
    "PM10":  {"alerta": 50.0, "elevado": 75.0, "unidade": "ug/m3"},
    "CO2":   {"alerta": 1000.0, "elevado": 2000.0, "unidade": "ppm"},
    "TEMP":  {"alerta": 32.0, "elevado": 38.0, "unidade": "C"},
    "RUIDO": {"alerta": 65.0, "elevado": 85.0, "unidade": "dB"},
    "HUM":   {"alerta": 80.0, "elevado": 90.0, "unidade": "%"},
    "AR":    {"alerta": 100.0, "elevado": 150.0, "unidade": "AQI"},
}


def classificar(tipo: str, valor: float) -> str:
    """Classifica o risco de um valor individual face aos limiares."""
    tipo = (tipo or "").upper()
    if tipo == "PM2.5":
        tipo = "PM25"

    limiar = LIMIARES.get(tipo)
    if limiar is None:
        return "NORMAL"

    if valor >= limiar["elevado"]:
        if tipo in ("PM25", "PM10", "AR"):
            return "RISCO_POLUICAO_ELEVADO"
        if tipo == "TEMP":
            return "TEMPERATURA_ELEVADA"
        if tipo == "RUIDO":
            return "RUIDO_EXCESSIVO"
        if tipo == "CO2":
            return "CO2_PERIGOSO"
        return "RISCO_ELEVADO"

    if valor >= limiar["alerta"]:
        return "ALERTA"

    return "NORMAL"


def tendencia(valores):
    """Deteta a tendencia comparando a 1.a metade com a 2.a metade da serie."""
    if len(valores) < 4:
        return "INSUFICIENTE"
    meio = len(valores) // 2
    media_inicio = statistics.fmean(valores[:meio])
    media_fim = statistics.fmean(valores[meio:])
    delta = media_fim - media_inicio
    margem = abs(media_inicio) * 0.05 if media_inicio else 0.01
    if delta > margem:
        return "A_SUBIR"
    if delta < -margem:
        return "A_DESCER"
    return "ESTAVEL"


def analisar_conjunto(tipo: str, valores):
    """Analise estatistica completa de um conjunto de medicoes."""
    if not valores:
        return {
            "Tipo": tipo, "Quantidade": 0, "Resultado": "SEM_DADOS",
            "Media": 0, "Minimo": 0, "Maximo": 0,
            "DesvioPadrao": 0, "Tendencia": "INSUFICIENTE",
        }

    media = round(statistics.fmean(valores), 2)
    minimo = round(min(valores), 2)
    maximo = round(max(valores), 2)
    desvio = round(statistics.pstdev(valores), 2) if len(valores) > 1 else 0.0
    tend = tendencia(valores)
    # O risco global e classificado pela media do periodo
    resultado = classificar(tipo, media)

    return {
        "Tipo": tipo,
        "Quantidade": len(valores),
        "Media": media,
        "Minimo": minimo,
        "Maximo": maximo,
        "DesvioPadrao": desvio,
        "Tendencia": tend,
        "Resultado": resultado,
    }


class Handler(BaseHTTPRequestHandler):
    def _responder(self, codigo, payload):
        corpo = json.dumps(payload).encode("utf-8")
        self.send_response(codigo)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(corpo)))
        self.end_headers()
        self.wfile.write(corpo)

    def _ler_json(self):
        tamanho = int(self.headers.get("Content-Length", 0))
        bruto = self.rfile.read(tamanho) if tamanho else b"{}"
        return json.loads(bruto.decode("utf-8"))

    def do_POST(self):
        try:
            dados = self._ler_json()

            # --- Compatibilidade com o SERVIDOR (analise em tempo real) ---
            if self.path == "/analyze":
                tipo = dados.get("Tipo", "")
                valor = float(dados.get("Valor", 0))
                resultado = classificar(tipo, valor)
                print(f"[ANALYSIS] /analyze  {tipo}={valor} -> {resultado}")
                self._responder(200, {
                    "Tipo": tipo,
                    "Valor": valor,
                    "Resultado": resultado,
                })
                return

            # --- Analise estatistica de um conjunto (pedida pela Interface) ---
            if self.path == "/analyze-batch":
                tipo = dados.get("Tipo", "")
                valores = [float(v) for v in dados.get("Valores", [])]
                resultado = analisar_conjunto(tipo, valores)
                print(f"[ANALYSIS] /analyze-batch  {tipo}  n={len(valores)} "
                      f"-> media={resultado['Media']} "
                      f"risco={resultado['Resultado']} "
                      f"tendencia={resultado['Tendencia']}")
                self._responder(200, resultado)
                return

            self._responder(404, {"erro": "endpoint desconhecido"})

        except Exception as exc:  # noqa: BLE001
            self._responder(500, {"erro": str(exc)})

    def log_message(self, *_args):
        # Silenciar o log HTTP por omissao (usamos prints proprios)
        pass


if __name__ == "__main__":
    servidor = ThreadingHTTPServer(("localhost", PORT), Handler)
    print(f"[ANALYSIS] Servico de Analise (Python) a escutar em "
          f"http://localhost:{PORT}")
    print("[ANALYSIS] Endpoints: POST /analyze  |  POST /analyze-batch")
    try:
        servidor.serve_forever()
    except KeyboardInterrupt:
        print("\n[ANALYSIS] Servico terminado.")
        servidor.shutdown()
