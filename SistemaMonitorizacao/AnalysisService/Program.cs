var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.MapPost("/analyze", (AnalysisData data) =>
{
    string resultado;

    if (data.Tipo == "PM25" && data.Valor > 35)
    {
        resultado = "RISCO_POLUICAO_ELEVADO";
    }
    else if (data.Tipo == "TEMP" && data.Valor > 35)
    {
        resultado = "TEMPERATURA_ELEVADA";
    }
    else
    {
        resultado = "NORMAL";
    }

    return Results.Ok(new
    {
        Tipo = data.Tipo,
        Valor = data.Valor,
        Resultado = resultado
    });
});

app.Run("http://localhost:7002");

record AnalysisData(
    string Tipo,
    double Valor
);