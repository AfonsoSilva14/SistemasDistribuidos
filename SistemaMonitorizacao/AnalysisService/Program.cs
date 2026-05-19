// AnalysisService — Serviço de Análise e Previsão (RPC via HTTP/JSON)
// ====================================================================
// Invocado remotamente pelo SERVIDOR (análise em tempo real) e pela
// Interface CLI (análise estatística de um conjunto de medições).
//
// Endpoints:
//   POST /analyze        {Tipo,Valor}            -> classificação de risco
//   POST /analyze-batch  {Tipo,Valores:[...]}    -> estatística completa
//
// Porta: 7002

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// --- Análise em tempo real (compatível com o Servidor) ---------------------
app.MapPost("/analyze", (AnalyzeRequest req) =>
{
    string resultado = Analise.Classificar(req.Tipo, req.Valor);
    Console.WriteLine($"[ANALYSIS] /analyze  {req.Tipo}={req.Valor} -> {resultado}");
    return Results.Ok(new
    {
        Tipo = req.Tipo,
        Valor = req.Valor,
        Resultado = resultado
    });
});

// --- Análise estatística de um conjunto (pedida pela Interface CLI) ---------
app.MapPost("/analyze-batch", (BatchRequest req) =>
{
    var r = Analise.AnalisarConjunto(req.Tipo, req.Valores ?? new List<double>());
    Console.WriteLine($"[ANALYSIS] /analyze-batch  {req.Tipo}  n={r.Quantidade} " +
                      $"-> media={r.Media} risco={r.Resultado} tendencia={r.Tendencia}");
    return Results.Ok(r);
});

app.Run("http://localhost:7002");


// ===========================================================================
// Lógica de análise
// ===========================================================================
static class Analise
{
    // Limiares de risco para a saúde pública (referências OMS / índices AQI)
    static readonly Dictionary<string, (double Alerta, double Elevado)> Limiares = new()
    {
        ["PM25"]  = (25.0, 35.0),
        ["PM10"]  = (50.0, 75.0),
        ["CO2"]   = (1000.0, 2000.0),
        ["TEMP"]  = (32.0, 38.0),
        ["RUIDO"] = (65.0, 85.0),
        ["HUM"]   = (80.0, 90.0),
        ["AR"]    = (100.0, 150.0),
    };

    public static string Classificar(string? tipo, double valor)
    {
        string t = (tipo ?? "").Trim().ToUpperInvariant();
        if (t == "PM2.5") t = "PM25";

        if (!Limiares.TryGetValue(t, out var lim))
            return "NORMAL";

        if (valor >= lim.Elevado)
        {
            return t switch
            {
                "PM25" or "PM10" or "AR" => "RISCO_POLUICAO_ELEVADO",
                "TEMP"  => "TEMPERATURA_ELEVADA",
                "RUIDO" => "RUIDO_EXCESSIVO",
                "CO2"   => "CO2_PERIGOSO",
                _       => "RISCO_ELEVADO"
            };
        }

        if (valor >= lim.Alerta)
            return "ALERTA";

        return "NORMAL";
    }

    // Tendência: compara a 1.ª metade com a 2.ª metade da série
    public static string Tendencia(List<double> valores)
    {
        if (valores.Count < 4) return "INSUFICIENTE";

        int meio = valores.Count / 2;
        double mediaInicio = valores.Take(meio).Average();
        double mediaFim = valores.Skip(meio).Average();
        double delta = mediaFim - mediaInicio;
        double margem = Math.Abs(mediaInicio) > 0 ? Math.Abs(mediaInicio) * 0.05 : 0.01;

        if (delta > margem) return "A_SUBIR";
        if (delta < -margem) return "A_DESCER";
        return "ESTAVEL";
    }

    public static BatchResult AnalisarConjunto(string? tipo, List<double> valores)
    {
        string t = (tipo ?? "").Trim().ToUpperInvariant();
        if (t == "PM2.5") t = "PM25";

        if (valores.Count == 0)
            return new BatchResult { Tipo = t, Quantidade = 0, Resultado = "SEM_DADOS",
                                     Tendencia = "INSUFICIENTE" };

        double media = Math.Round(valores.Average(), 2);
        double minimo = Math.Round(valores.Min(), 2);
        double maximo = Math.Round(valores.Max(), 2);

        double desvio = 0.0;
        if (valores.Count > 1)
        {
            double m = valores.Average();
            double variancia = valores.Sum(v => (v - m) * (v - m)) / valores.Count; // pop. stdev
            desvio = Math.Round(Math.Sqrt(variancia), 2);
        }

        return new BatchResult
        {
            Tipo = t,
            Quantidade = valores.Count,
            Media = media,
            Minimo = minimo,
            Maximo = maximo,
            DesvioPadrao = desvio,
            Tendencia = Tendencia(valores),
            Resultado = Classificar(t, media)   // risco classificado pela média do período
        };
    }
}

record AnalyzeRequest(string Tipo, double Valor);
record BatchRequest(string Tipo, List<double>? Valores);

class BatchResult
{
    public string Tipo { get; set; } = "";
    public int Quantidade { get; set; }
    public double Media { get; set; }
    public double Minimo { get; set; }
    public double Maximo { get; set; }
    public double DesvioPadrao { get; set; }
    public string Tendencia { get; set; } = "";
    public string Resultado { get; set; } = "";
}
