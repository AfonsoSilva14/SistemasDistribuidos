// PreProcessingService — Serviço de Pré-processamento (RPC via HTTP/JSON)
// =======================================================================
// Invocado remotamente pelo GATEWAY antes da agregação.
// Responsável pela UNIFORMIZAÇÃO dos dados:
//   - Normalização de nomes de tipo (PM2.5 -> PM25)
//   - Conversão de escalas/unidades para a unidade canónica de cada tipo
//   - Arredondamento a 2 casas decimais
//
// Porta: 7001

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapPost("/preprocess", (SensorData data) =>
{
    string tipo = (data.Tipo ?? "").Trim().ToUpperInvariant();

    // 1. Normalização do nome do tipo
    if (tipo == "PM2.5")
        tipo = "PM25";

    double valor = data.Valor;
    string unidade = (data.Unidade ?? "").Trim();

    // 2. Conversão de escalas/unidades para a unidade canónica
    (valor, unidade) = Uniformizar(tipo, valor, unidade);

    // 3. Arredondamento
    valor = Math.Round(valor, 2);

    Console.WriteLine($"[PREPROC] {data.SensorId} {tipo}={valor}{unidade} (normalizado)");

    return Results.Ok(new
    {
        SensorId = data.SensorId,
        Tipo = tipo,
        Valor = valor,
        Unidade = unidade
    });
});

app.Run("http://localhost:7001");

// Converte o par (valor, unidade) para a unidade canónica do tipo.
static (double, string) Uniformizar(string tipo, double valor, string unidade)
{
    string u = unidade.ToLowerInvariant();

    switch (tipo)
    {
        case "TEMP":
            // Canónico: C. Converte Fahrenheit e Kelvin se vierem nessas escalas.
            if (u is "f" or "ºf" or "°f")
                return ((valor - 32) * 5.0 / 9.0, "C");
            if (u is "k")
                return (valor - 273.15, "C");
            return (valor, "C"); // C / ºC / °C -> C

        case "PM25":
        case "PM10":
        case "AR":
            // Canónico: ug/m3 (µ -> u). Converte mg/m3 -> ug/m3.
            if (u is "mg/m3")
                return (valor * 1000.0, "ug/m3");
            if (u is "µg/m3" or "ug/m3")
                return (valor, "ug/m3");
            return (valor, unidade);

        case "PRESS":
            // Canónico: hPa. Converte Pa e kPa.
            if (u is "pa")
                return (valor / 100.0, "hPa");
            if (u is "kpa")
                return (valor * 10.0, "hPa");
            return (valor, "hPa");

        case "RUIDO":
            return (valor, "dB"); // dB / dBA -> dB

        case "HUM":
            return (valor, "%");

        case "CO2":
            return (valor, "ppm");

        case "LUZ":
            return (valor, "lux"); // lux / lx -> lux

        default:
            return (valor, unidade);
    }
}

record SensorData(
    string SensorId,
    string Tipo,
    double Valor,
    string Unidade
);
