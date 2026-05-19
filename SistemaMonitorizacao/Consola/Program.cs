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
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

Console.OutputEncoding = Encoding.UTF8;

// Caminho para a BD do Servidor (1.º argumento ou caminho relativo por omissão)
string dbPath = args.Length > 0 ? args[0] : "../Servidor/servidor.db";
string dbConnectionString = $"Data Source={dbPath}";
const string analysisUrl = "http://localhost:7002/analyze-batch";

Console.WriteLine("=================================================");
Console.WriteLine(" INTERFACE DE VISUALIZAÇÃO E ANÁLISE — One Health ");
Console.WriteLine("=================================================");
Console.WriteLine($"Base de dados: {dbPath}");
Console.WriteLine();

bool running = true;
while (running)
{
    Console.WriteLine("=== MENU ===");
    Console.WriteLine("1 - Ver medições (com filtros)");
    Console.WriteLine("2 - Pedir nova análise estatística (parametrizada)");
    Console.WriteLine("3 - Ver histórico de análises");
    Console.WriteLine("4 - Resumo geral (contagem por tipo)");
    Console.WriteLine("0 - Sair");
    Console.Write("Opção: ");
    string? op = Console.ReadLine();
    Console.WriteLine();

    switch (op)
    {
        case "1": VerMedicoes(dbConnectionString); break;
        case "2": await PedirAnalise(dbConnectionString, analysisUrl); break;
        case "3": VerHistoricoAnalises(dbConnectionString); break;
        case "4": ResumoGeral(dbConnectionString); break;
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
