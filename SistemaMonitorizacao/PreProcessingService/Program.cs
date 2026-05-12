var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapPost("/preprocess", (SensorData data) =>
{
    // Exemplo simples de pré-processamento

    string tipo = data.Tipo.ToUpper();

    // Normalizar PM2.5 → PM25
    if (tipo == "PM2.5")
        tipo = "PM25";

    // Arredondar valor
    double valor = Math.Round(data.Valor, 2);

    return Results.Ok(new
    {
        SensorId = data.SensorId,
        Tipo = tipo,
        Valor = valor,
        Unidade = data.Unidade
    });
});

app.Run("http://localhost:7001");

record SensorData(
    string SensorId,
    string Tipo,
    double Valor,
    string Unidade
);