// Interface CLI — Consola de Visualização e Análise (TP2)
// =========================================================
// Interface de linha de comandos que permite:
//   1. Consultar as medições persistidas (filtros: tipo, sensor, intervalo)
//   2. Pedir uma NOVA análise estatística parametrizada — invoca o
//      AnalysisService (RPC, Python) e persiste o resultado na BD
//   3. Consultar o histórico de análises já realizadas
//
// Lê diretamente a base de dados do Servidor (servidor.db).

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

Console.OutputEncoding = Encoding.UTF8;

// Caminho para a BD do Servidor.
// O servidor.db é criado na pasta de trabalho do Servidor — que difere
// consoante seja lançado por `dotnet run` (Servidor/servidor.db) ou pelo
// Visual Studio (Servidor/bin/Debug/net8.0/servidor.db). Procuramos em
// todas as localizações plausíveis, subindo a árvore de diretórios.
string dbPath = ResolverCaminhoBD(args);
string dbConnectionString = $"Data Source={dbPath}";
const string analysisUrl = "http://localhost:7002/analyze-batch";

static string ResolverCaminhoBD(string[] args)
{
    if (args.Length > 0 && File.Exists(args[0]))
        return Path.GetFullPath(args[0]);

    var candidatos = new List<string>
    {
        "servidor.db",
        Path.Combine("..", "Servidor", "servidor.db"),
        Path.Combine("..", "Servidor", "bin", "Debug", "net8.0", "servidor.db"),
    };

    // Sobe a árvore a partir do executável e do diretório atual
    foreach (var raiz in new[] { AppContext.BaseDirectory, Directory.GetCurrentDirectory() })
    {
        var dir = new DirectoryInfo(raiz);
        while (dir != null)
        {
            candidatos.Add(Path.Combine(dir.FullName, "servidor.db"));
            candidatos.Add(Path.Combine(dir.FullName, "Servidor", "servidor.db"));
            candidatos.Add(Path.Combine(dir.FullName, "Servidor", "bin", "Debug", "net8.0", "servidor.db"));
            candidatos.Add(Path.Combine(dir.FullName, "SistemaMonitorizacao", "Servidor", "servidor.db"));
            candidatos.Add(Path.Combine(dir.FullName, "SistemaMonitorizacao", "Servidor", "bin", "Debug", "net8.0", "servidor.db"));
            dir = dir.Parent;
        }
    }

    foreach (var c in candidatos)
        if (File.Exists(c))
            return Path.GetFullPath(c);

    // Nada encontrado: devolve o melhor palpite (erro será claro)
    return args.Length > 0 ? args[0] : Path.Combine("..", "Servidor", "servidor.db");
}

Console.WriteLine("=================================================");
Console.WriteLine(" INTERFACE DE VISUALIZAÇÃO E ANÁLISE — One Health ");
Console.WriteLine("=================================================");
if (File.Exists(dbPath))
    Console.WriteLine($"Base de dados: {dbPath}");
else
    Console.WriteLine($"AVISO: não encontrei servidor.db. Arranca o Servidor primeiro " +
                      $"(ou passa o caminho como argumento). Tentado: {dbPath}");
Console.WriteLine();

bool running = true;
while (running)
{
    Console.WriteLine("=== MENU ===");
    Console.WriteLine("1 - Ver medições (com filtros)");
    Console.WriteLine("2 - Pedir nova análise estatística (parametrizada)");
    Console.WriteLine("3 - Ver histórico de análises");
    Console.WriteLine("4 - Resumo geral (contagem por tipo)");
    Console.WriteLine("5 - Dashboard operacional");
    Console.WriteLine("6 - Ver alertas");
    Console.WriteLine("7 - Exportar dados");
    Console.WriteLine("8 - Marcar alerta como resolvido");
    Console.WriteLine("0 - Sair");
    Console.Write("Opção: ");
    string? op = Console.ReadLine()?.Trim();
    Console.WriteLine();

    switch (op)
    {
        case "1": VerMedicoes(dbConnectionString); break;
        case "2": await PedirAnalise(dbConnectionString, analysisUrl); break;
        case "3": VerHistoricoAnalises(dbConnectionString); break;
        case "4": ResumoGeral(dbConnectionString); break;
        case "5": DashboardOperacional(dbConnectionString); break;
        case "6": VerAlertas(dbConnectionString); break;
        case "7": ExportarDados(dbConnectionString); break;
        case "8": MarcarAlertaResolvido(dbConnectionString); break;
        case "0": running = false; break;
        default: Console.WriteLine("Opção inválida."); break;
    }
    Console.WriteLine();
}

Console.WriteLine("[CONSOLA] Terminada.");


// ---------------------------------------------------------------------------
// 1. Ver medições com filtros opcionais
// ---------------------------------------------------------------------------
static void VerMedicoes(string cs)
{
    Console.Write("Filtrar por Tipo (ENTER = todos): ");
    string tipo = (Console.ReadLine() ?? "").Trim();
    Console.Write("Filtrar por SensorId (ENTER = todos): ");
    string sensor = (Console.ReadLine() ?? "").Trim();
    Console.Write("Máximo de linhas (ENTER = 20): ");
    string maxTxt = (Console.ReadLine() ?? "").Trim();
    int max = int.TryParse(maxTxt, out int m) ? m : 20;

    var filtros = new List<string>();
    if (tipo != "") filtros.Add("Tipo = @Tipo");
    if (sensor != "") filtros.Add("SensorId = @Sensor");
    string where = filtros.Count > 0 ? "WHERE " + string.Join(" AND ", filtros) : "";

    string sql = $@"SELECT TimestampMedicao, SensorId, Zona, Tipo, Valor, Unidade
                    FROM MedicoesServidor {where}
                    ORDER BY Id DESC LIMIT @Max";

    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();
        using var cmd = new SqliteCommand(sql, con);
        if (tipo != "") cmd.Parameters.AddWithValue("@Tipo", tipo);
        if (sensor != "") cmd.Parameters.AddWithValue("@Sensor", sensor);
        cmd.Parameters.AddWithValue("@Max", max);

        using var r = cmd.ExecuteReader();
        Console.WriteLine();
        Console.WriteLine($"{"Timestamp",-20} {"Sensor",-7} {"Zona",-14} {"Tipo",-7} {"Valor",10} {"Un",-6}");
        Console.WriteLine(new string('-', 70));
        int n = 0;
        while (r.Read())
        {
            Console.WriteLine($"{r.GetString(0),-20} {r.GetString(1),-7} {r.GetString(2),-14} " +
                              $"{r.GetString(3),-7} {r.GetDouble(4),10:F2} {r.GetString(5),-6}");
            n++;
        }
        Console.WriteLine(new string('-', 70));
        Console.WriteLine($"{n} medição(ões).");
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}


// ---------------------------------------------------------------------------
// 2. Pedir nova análise — query BD → RPC AnalysisService → persistir resultado
// ---------------------------------------------------------------------------
static async Task PedirAnalise(string cs, string analysisUrl)
{
    Console.Write("Tipo a analisar (ex: TEMP, PM25): ");
    string tipo = (Console.ReadLine() ?? "").Trim().ToUpperInvariant();
    if (tipo == "") { Console.WriteLine("Tipo obrigatório."); return; }

    Console.Write("SensorId (ENTER = todos): ");
    string sensor = (Console.ReadLine() ?? "").Trim();
    Console.Write("Data/hora início (yyyy-MM-ddTHH:mm:ss, ENTER = sem limite): ");
    string ini = (Console.ReadLine() ?? "").Trim();
    Console.Write("Data/hora fim    (yyyy-MM-ddTHH:mm:ss, ENTER = sem limite): ");
    string fim = (Console.ReadLine() ?? "").Trim();

    var filtros = new List<string> { "Tipo = @Tipo" };
    if (sensor != "") filtros.Add("SensorId = @Sensor");
    if (ini != "") filtros.Add("TimestampMedicao >= @Ini");
    if (fim != "") filtros.Add("TimestampMedicao <= @Fim");
    string where = "WHERE " + string.Join(" AND ", filtros);

    var valores = new List<double>();
    string zona = "";
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();
        using var cmd = new SqliteCommand(
            $"SELECT Valor, Zona FROM MedicoesServidor {where} ORDER BY TimestampMedicao", con);
        cmd.Parameters.AddWithValue("@Tipo", tipo);
        if (sensor != "") cmd.Parameters.AddWithValue("@Sensor", sensor);
        if (ini != "") cmd.Parameters.AddWithValue("@Ini", ini);
        if (fim != "") cmd.Parameters.AddWithValue("@Fim", fim);

        using var r = cmd.ExecuteReader();
        while (r.Read()) { valores.Add(r.GetDouble(0)); zona = r.GetString(1); }
    }
    catch (Exception ex) { Console.WriteLine("Erro BD: " + ex.Message); return; }

    if (valores.Count == 0) { Console.WriteLine("Sem medições para esses filtros."); return; }
    Console.WriteLine($"\n{valores.Count} medição(ões) encontradas. A invocar serviço de análise (RPC)...");

    // RPC → AnalysisService (Python)
    BatchResult? res;
    try
    {
        using var http = new HttpClient();
        var pedido = new { Tipo = tipo, Valores = valores };
        var resp = await http.PostAsJsonAsync(analysisUrl, pedido);
        if (!resp.IsSuccessStatusCode) { Console.WriteLine("Serviço de análise devolveu erro."); return; }
        res = await resp.Content.ReadFromJsonAsync<BatchResult>();
    }
    catch (Exception ex)
    {
        Console.WriteLine("Não foi possível contactar o AnalysisService (está a correr na porta 7002?): " + ex.Message);
        return;
    }
    if (res == null) { Console.WriteLine("Resposta inválida do serviço."); return; }

    Console.WriteLine();
    Console.WriteLine("========= RESULTADO DA ANÁLISE =========");
    Console.WriteLine($" Tipo .......... {res.Tipo}");
    Console.WriteLine($" Quantidade .... {res.Quantidade}");
    Console.WriteLine($" Média ......... {res.Media:F2}");
    Console.WriteLine($" Mínimo ........ {res.Minimo:F2}");
    Console.WriteLine($" Máximo ........ {res.Maximo:F2}");
    Console.WriteLine($" Desvio-padrão . {res.DesvioPadrao:F2}");
    Console.WriteLine($" Tendência ..... {res.Tendencia}");
    Console.WriteLine($" Risco ......... {res.Resultado}");
    Console.WriteLine("========================================");

    // Persistir o resultado na tabela Analises
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();
        string sql = @"INSERT INTO Analises
            (TimestampAnalise, Tipo, SensorId, Zona, PeriodoInicio, PeriodoFim,
             Quantidade, Media, Minimo, Maximo, DesvioPadrao, Tendencia, Resultado)
            VALUES (@T,@Tp,@S,@Z,@PI,@PF,@Q,@Me,@Mi,@Ma,@D,@Te,@R)";
        using var cmd = new SqliteCommand(sql, con);
        cmd.Parameters.AddWithValue("@T", DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"));
        cmd.Parameters.AddWithValue("@Tp", tipo);
        cmd.Parameters.AddWithValue("@S", sensor == "" ? "TODOS" : sensor);
        cmd.Parameters.AddWithValue("@Z", zona == "" ? "" : zona);
        cmd.Parameters.AddWithValue("@PI", ini == "" ? "" : ini);
        cmd.Parameters.AddWithValue("@PF", fim == "" ? "" : fim);
        cmd.Parameters.AddWithValue("@Q", res.Quantidade);
        cmd.Parameters.AddWithValue("@Me", res.Media);
        cmd.Parameters.AddWithValue("@Mi", res.Minimo);
        cmd.Parameters.AddWithValue("@Ma", res.Maximo);
        cmd.Parameters.AddWithValue("@D", res.DesvioPadrao);
        cmd.Parameters.AddWithValue("@Te", res.Tendencia);
        cmd.Parameters.AddWithValue("@R", res.Resultado);
        cmd.ExecuteNonQuery();
        Console.WriteLine("(Resultado guardado na tabela Analises.)");
    }
    catch (Exception ex) { Console.WriteLine("Aviso: não persistiu — " + ex.Message); }
}


// ---------------------------------------------------------------------------
// 3. Histórico de análises
// ---------------------------------------------------------------------------
static void VerHistoricoAnalises(string cs)
{
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();
        using var cmd = new SqliteCommand(
            @"SELECT TimestampAnalise, Tipo, SensorId, Quantidade, Media,
                     Minimo, Maximo, Tendencia, Resultado
              FROM Analises ORDER BY Id DESC LIMIT 30", con);
        using var r = cmd.ExecuteReader();
        Console.WriteLine($"{"Quando",-20} {"Tipo",-6} {"Sensor",-7} {"N",4} {"Média",9} {"Tend.",-10} {"Risco",-22}");
        Console.WriteLine(new string('-', 88));
        int n = 0;
        while (r.Read())
        {
            Console.WriteLine($"{r.GetString(0),-20} {r.GetString(1),-6} {r.GetString(2),-7} " +
                              $"{r.GetInt32(3),4} {r.GetDouble(4),9:F2} {r.GetString(7),-10} {r.GetString(8),-22}");
            n++;
        }
        Console.WriteLine(new string('-', 88));
        Console.WriteLine($"{n} análise(s) no histórico.");
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}


// ---------------------------------------------------------------------------
// 4. Resumo geral
// ---------------------------------------------------------------------------
static void ResumoGeral(string cs)
{
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();
        using var cmd = new SqliteCommand(
            @"SELECT Tipo, COUNT(*), ROUND(AVG(Valor),2), ROUND(MIN(Valor),2), ROUND(MAX(Valor),2)
              FROM MedicoesServidor GROUP BY Tipo ORDER BY Tipo", con);
        using var r = cmd.ExecuteReader();
        Console.WriteLine($"{"Tipo",-8} {"Qtd",6} {"Média",10} {"Mín",10} {"Máx",10}");
        Console.WriteLine(new string('-', 48));
        while (r.Read())
            Console.WriteLine($"{r.GetString(0),-8} {r.GetInt32(1),6} {r.GetDouble(2),10:F2} " +
                              $"{r.GetDouble(3),10:F2} {r.GetDouble(4),10:F2}");
        Console.WriteLine(new string('-', 48));
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}

// ---------------------------------------------------------------------------
// 5. Dashboard operacional
// ---------------------------------------------------------------------------
static void DashboardOperacional(string cs)
{
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();

        int totalMedicoes = ExecutarScalarInt(con, "SELECT COUNT(*) FROM MedicoesServidor");
        int sensores = ExecutarScalarInt(con, "SELECT COUNT(DISTINCT SensorId) FROM MedicoesServidor");
        int zonas = ExecutarScalarInt(con, "SELECT COUNT(DISTINCT Zona) FROM MedicoesServidor");
        int alertas = TabelaExiste(con, "Alertas")
            ? ExecutarScalarInt(con, "SELECT COUNT(*) FROM Alertas WHERE Resolvido = 0")
            : 0;

        Console.WriteLine("========= DASHBOARD =========");
        Console.WriteLine($" Medicoes ........ {totalMedicoes}");
        Console.WriteLine($" Sensores ........ {sensores}");
        Console.WriteLine($" Zonas ........... {zonas}");
        Console.WriteLine($" Alertas ativos .. {alertas}");
        Console.WriteLine("=============================");

        using var cmd = new SqliteCommand(
            @"SELECT SensorId, Zona, MAX(TimestampMedicao), COUNT(*)
              FROM MedicoesServidor
              GROUP BY SensorId, Zona
              ORDER BY MAX(TimestampMedicao) DESC", con);
        using var r = cmd.ExecuteReader();

        Console.WriteLine();
        Console.WriteLine($"{"Sensor",-8} {"Zona",-14} {"Ultima medicao",-20} {"Qtd",6}");
        Console.WriteLine(new string('-', 56));
        while (r.Read())
        {
            Console.WriteLine($"{r.GetString(0),-8} {r.GetString(1),-14} {r.GetString(2),-20} {r.GetInt32(3),6}");
        }
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}

// ---------------------------------------------------------------------------
// 6. Alertas
// ---------------------------------------------------------------------------
static void VerAlertas(string cs)
{
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();
        if (!TabelaExiste(con, "Alertas"))
        {
            Console.WriteLine("Tabela Alertas ainda nao existe. Arranca o Servidor atualizado primeiro.");
            return;
        }

        using var cmd = new SqliteCommand(
            @"SELECT Id, TimestampAlerta, SensorId, Zona, Tipo, Valor, Unidade, Resultado, Resolvido
              FROM Alertas
              ORDER BY Id DESC LIMIT 30", con);
        using var r = cmd.ExecuteReader();

        Console.WriteLine($"{"Id",4} {"Quando",-20} {"Sensor",-8} {"Zona",-14} {"Tipo",-7} {"Valor",9} {"Un",-6} {"Resultado",-24} {"Res.",5}");
        Console.WriteLine(new string('-', 106));
        int n = 0;
        while (r.Read())
        {
            Console.WriteLine($"{r.GetInt32(0),4} {r.GetString(1),-20} {r.GetString(2),-8} {r.GetString(3),-14} " +
                              $"{r.GetString(4),-7} {r.GetDouble(5),9:F2} {r.GetString(6),-6} {r.GetString(7),-24} {r.GetInt32(8),5}");
            n++;
        }
        Console.WriteLine(new string('-', 106));
        Console.WriteLine($"{n} alerta(s).");
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}

// ---------------------------------------------------------------------------
// 7. Exportacao de dados
// ---------------------------------------------------------------------------
static void ExportarDados(string cs)
{
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();

        ExportarCsv(
            con,
            "medicoes_export.csv",
            @"SELECT TimestampMedicao, GatewayId, SensorId, Zona, Tipo, Valor, Unidade
              FROM MedicoesServidor ORDER BY Id",
            new[] { "TimestampMedicao", "GatewayId", "SensorId", "Zona", "Tipo", "Valor", "Unidade" });

        ExportarJson(
            con,
            "analises_export.json",
            @"SELECT TimestampAnalise, Tipo, SensorId, Zona, Quantidade, Media, Minimo, Maximo,
                     DesvioPadrao, Tendencia, Resultado
              FROM Analises ORDER BY Id");

        if (TabelaExiste(con, "Alertas"))
        {
            ExportarCsv(
                con,
                "alertas_export.csv",
                @"SELECT TimestampAlerta, GatewayId, SensorId, Zona, Tipo, Valor, Unidade, Resultado, Resolvido
                  FROM Alertas ORDER BY Id",
                new[] { "TimestampAlerta", "GatewayId", "SensorId", "Zona", "Tipo", "Valor", "Unidade", "Resultado", "Resolvido" });
        }

        Console.WriteLine("Exportacao concluida: medicoes_export.csv, analises_export.json e alertas_export.csv.");
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}

// ---------------------------------------------------------------------------
// 8. Resolver alertas
// ---------------------------------------------------------------------------
static void MarcarAlertaResolvido(string cs)
{
    try
    {
        using var con = new SqliteConnection(cs);
        con.Open();
        if (!TabelaExiste(con, "Alertas"))
        {
            Console.WriteLine("Tabela Alertas ainda nao existe. Arranca o Servidor atualizado primeiro.");
            return;
        }

        Console.Write("Id do alerta a resolver: ");
        string idTxt = (Console.ReadLine() ?? "").Trim();
        if (!int.TryParse(idTxt, out int id) || id <= 0)
        {
            Console.WriteLine("Id invalido.");
            return;
        }

        using var cmd = new SqliteCommand(
            "UPDATE Alertas SET Resolvido = 1 WHERE Id = @Id", con);
        cmd.Parameters.AddWithValue("@Id", id);
        int afetados = cmd.ExecuteNonQuery();

        Console.WriteLine(afetados == 0
            ? "Alerta nao encontrado."
            : "Alerta marcado como resolvido.");
    }
    catch (Exception ex) { Console.WriteLine("Erro: " + ex.Message); }
}

static int ExecutarScalarInt(SqliteConnection con, string sql)
{
    using var cmd = new SqliteCommand(sql, con);
    object? value = cmd.ExecuteScalar();
    return Convert.ToInt32(value ?? 0, CultureInfo.InvariantCulture);
}

static bool TabelaExiste(SqliteConnection con, string nome)
{
    using var cmd = new SqliteCommand(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = @Nome", con);
    cmd.Parameters.AddWithValue("@Nome", nome);
    return Convert.ToInt32(cmd.ExecuteScalar() ?? 0, CultureInfo.InvariantCulture) > 0;
}

static void ExportarCsv(SqliteConnection con, string caminho, string sql, string[] headers)
{
    var linhas = new List<string> { string.Join(",", headers.Select(EscaparCsv)) };
    using var cmd = new SqliteCommand(sql, con);
    using var r = cmd.ExecuteReader();
    while (r.Read())
    {
        var valores = new List<string>();
        for (int i = 0; i < r.FieldCount; i++)
            valores.Add(EscaparCsv(r.GetValue(i)?.ToString() ?? ""));
        linhas.Add(string.Join(",", valores));
    }

    File.WriteAllLines(caminho, linhas, Encoding.UTF8);
}

static void ExportarJson(SqliteConnection con, string caminho, string sql)
{
    var linhas = new List<Dictionary<string, object?>>();
    using var cmd = new SqliteCommand(sql, con);
    using var r = cmd.ExecuteReader();
    while (r.Read())
    {
        var item = new Dictionary<string, object?>();
        for (int i = 0; i < r.FieldCount; i++)
            item[r.GetName(i)] = r.IsDBNull(i) ? null : r.GetValue(i);
        linhas.Add(item);
    }

    var json = JsonSerializer.Serialize(linhas, new JsonSerializerOptions { WriteIndented = true });
    File.WriteAllText(caminho, json, Encoding.UTF8);
}

static string EscaparCsv(string valor)
{
    if (valor.Contains('"') || valor.Contains(',') || valor.Contains('\n') || valor.Contains('\r'))
        return "\"" + valor.Replace("\"", "\"\"") + "\"";
    return valor;
}


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
