using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

Console.OutputEncoding = Encoding.UTF8;

const int serverPort = 5000;
string dbConnectionString = "Data Source=servidor.db";

// Um lock por tipo de dado → acesso sequencial por ficheiro
var fileLocks = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

InicializarBaseDeDados(dbConnectionString);

TcpListener listener = new TcpListener(IPAddress.Loopback, serverPort);
listener.Start();

Console.WriteLine($"[SERVIDOR] À escuta na porta {serverPort}...");

while (true)
{
    TcpClient gatewayClient = listener.AcceptTcpClient();
    Console.WriteLine("[SERVIDOR] Gateway ligado.");

    _ = Task.Run(async () =>
        await HandleGateway(gatewayClient, dbConnectionString, fileLocks));
}

static async Task HandleGateway(
    TcpClient client,
    string dbConnectionString,
    ConcurrentDictionary<string, object> fileLocks)
{
    try
    {
        using NetworkStream ns = client.GetStream();
        using StreamReader reader = new StreamReader(ns, Encoding.UTF8);
        using StreamWriter writer = new StreamWriter(ns, Encoding.UTF8)
        {
            AutoFlush = true
        };

        string? line;

        while ((line = reader.ReadLine()) != null)
        {
            Console.WriteLine($"[SERVIDOR] Recebido: {line}");

            string[] parts = line.Split('|');
            string command = parts[0];

            switch (command)
            {
                case "GW_HELLO":
                    {
                        if (parts.Length >= 3)
                        {
                            string gatewayId = parts[1];
                            writer.WriteLine($"GW_HELLO_ACK|{gatewayId}|OK");
                        }
                        else
                        {
                            writer.WriteLine("SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                        }

                        break;
                    }

                case "GW_DATA":
                    {
                        if (parts.Length >= 8)
                        {
                            string gatewayId = parts[1];
                            string sensorId = parts[2];
                            string zona = parts[3];
                            string timestamp = parts[4];
                            string tipo = parts[5];
                            string valor = parts[6];
                            string unidade = parts[7];

                            string logLine =
                                $"{timestamp}|{gatewayId}|{sensorId}|{zona}|{tipo}|{valor}|{unidade}";

                            // Guarda em ficheiro específico do tipo
                            string tipoFicheiro =
                                $"dados_{tipo.ToUpperInvariant()}.txt";

                            var lockObj = fileLocks.GetOrAdd(tipo, _ => new object());

                            lock (lockObj)
                            {
                                File.AppendAllText(
                                    tipoFicheiro,
                                    logLine + Environment.NewLine);
                            }

                            GuardarMedicaoServidorNaBaseDeDados(
                                dbConnectionString,
                                gatewayId,
                                sensorId,
                                zona,
                                timestamp,
                                tipo,
                                valor,
                                unidade);

                            // RPC → AnalysisService
                            double valorDouble =
                                double.Parse(valor, CultureInfo.InvariantCulture);

                            string resultadoAnalise =
                                await ChamarAnalysisService(tipo, valorDouble);

                            // Persistir o resultado da análise na base de dados
                            GuardarAnaliseNaBaseDeDados(
                                dbConnectionString,
                                DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss"),
                                tipo, sensorId, zona,
                                periodoInicio: timestamp,
                                periodoFim: timestamp,
                                quantidade: 1,
                                media: valorDouble,
                                minimo: valorDouble,
                                maximo: valorDouble,
                                desvioPadrao: 0,
                                tendencia: "TEMPO_REAL",
                                resultado: resultadoAnalise);

                            Console.WriteLine(
                                $"[SERVIDOR] Resultado da análise: {resultadoAnalise} (guardado na BD)");

                            Console.WriteLine(
                                $"[SERVIDOR] Guardado em {tipoFicheiro}: {logLine}");

                            writer.WriteLine(
                                $"SERVER_ACK|{gatewayId}|{sensorId}|{timestamp}|OK");
                        }
                        else
                        {
                            writer.WriteLine(
                                "SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                        }

                        break;
                    }

                case "GW_BYE":
                    {
                        if (parts.Length >= 3)
                        {
                            string gatewayId = parts[1];
                            string timestamp = parts[2];

                            writer.WriteLine(
                                $"GW_BYE_ACK|{gatewayId}|{timestamp}|OK");
                        }
                        else
                        {
                            writer.WriteLine(
                                "SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                        }

                        Console.WriteLine(
                            "[SERVIDOR] Gateway terminou comunicação.");

                        break;
                    }

                case "GW_HEARTBEAT":
                    {
                        if (parts.Length >= 3)
                        {
                            string gatewayId = parts[1];
                            string timestamp = parts[2];

                            writer.WriteLine(
                                $"GW_HEARTBEAT_ACK|{gatewayId}|{timestamp}|OK");
                        }
                        else
                        {
                            writer.WriteLine(
                                "SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                        }

                        break;
                    }

                default:
                    {
                        writer.WriteLine(
                            "SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|UNKNOWN_COMMAND");

                        break;
                    }
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[SERVIDOR] Erro: {ex.Message}");
    }
    finally
    {
        client.Close();
        Console.WriteLine("[SERVIDOR] Ligação fechada.");
    }
}

static async Task<string> ChamarAnalysisService(
    string tipo,
    double valor)
{
    try
    {
        using HttpClient http = new HttpClient();

        var pedido = new
        {
            Tipo = tipo,
            Valor = valor
        };

        var resposta = await http.PostAsJsonAsync(
            "http://localhost:7002/analyze",
            pedido);

        if (!resposta.IsSuccessStatusCode)
            return "ERRO_ANALISE";

        var resultado =
            await resposta.Content.ReadFromJsonAsync<AnalysisResponse>();

        return resultado?.Resultado ?? "SEM_RESULTADO";
    }
    catch
    {
        return "ANALYSIS_SERVICE_OFFLINE";
    }
}

static void InicializarBaseDeDados(string dbConnectionString)
{
    using var connection = new SqliteConnection(dbConnectionString);

    connection.Open();

    string createMedicoes = @"
CREATE TABLE IF NOT EXISTS MedicoesServidor (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    GatewayId TEXT NOT NULL,
    SensorId TEXT NOT NULL,
    Zona TEXT NOT NULL,
    TimestampMedicao TEXT NOT NULL,
    Tipo TEXT NOT NULL,
    Valor REAL NOT NULL,
    Unidade TEXT NOT NULL
)";

    using (var cmd = new SqliteCommand(createMedicoes, connection))
        cmd.ExecuteNonQuery();

    string createAnalises = @"
CREATE TABLE IF NOT EXISTS Analises (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    TimestampAnalise TEXT NOT NULL,
    Tipo TEXT NOT NULL,
    SensorId TEXT,
    Zona TEXT,
    PeriodoInicio TEXT,
    PeriodoFim TEXT,
    Quantidade INTEGER NOT NULL,
    Media REAL,
    Minimo REAL,
    Maximo REAL,
    DesvioPadrao REAL,
    Tendencia TEXT,
    Resultado TEXT NOT NULL
)";

    using (var cmd = new SqliteCommand(createAnalises, connection))
        cmd.ExecuteNonQuery();
}

static void GuardarAnaliseNaBaseDeDados(
    string connectionString,
    string timestampAnalise,
    string tipo,
    string sensorId,
    string zona,
    string periodoInicio,
    string periodoFim,
    int quantidade,
    double media,
    double minimo,
    double maximo,
    double desvioPadrao,
    string tendencia,
    string resultado)
{
    using var connection = new SqliteConnection(connectionString);
    connection.Open();

    string sql = @"
INSERT INTO Analises
    (TimestampAnalise, Tipo, SensorId, Zona, PeriodoInicio, PeriodoFim,
     Quantidade, Media, Minimo, Maximo, DesvioPadrao, Tendencia, Resultado)
VALUES
    (@TimestampAnalise, @Tipo, @SensorId, @Zona, @PeriodoInicio, @PeriodoFim,
     @Quantidade, @Media, @Minimo, @Maximo, @DesvioPadrao, @Tendencia, @Resultado)";

    using var cmd = new SqliteCommand(sql, connection);
    cmd.Parameters.AddWithValue("@TimestampAnalise", timestampAnalise);
    cmd.Parameters.AddWithValue("@Tipo", tipo);
    cmd.Parameters.AddWithValue("@SensorId", sensorId);
    cmd.Parameters.AddWithValue("@Zona", zona);
    cmd.Parameters.AddWithValue("@PeriodoInicio", periodoInicio);
    cmd.Parameters.AddWithValue("@PeriodoFim", periodoFim);
    cmd.Parameters.AddWithValue("@Quantidade", quantidade);
    cmd.Parameters.AddWithValue("@Media", media);
    cmd.Parameters.AddWithValue("@Minimo", minimo);
    cmd.Parameters.AddWithValue("@Maximo", maximo);
    cmd.Parameters.AddWithValue("@DesvioPadrao", desvioPadrao);
    cmd.Parameters.AddWithValue("@Tendencia", tendencia);
    cmd.Parameters.AddWithValue("@Resultado", resultado);

    cmd.ExecuteNonQuery();
}

static void GuardarMedicaoServidorNaBaseDeDados(
    string connectionString,
    string gatewayId,
    string sensorId,
    string zona,
    string timestamp,
    string tipo,
    string valorTexto,
    string unidade)
{
    double valor =
        double.Parse(valorTexto, CultureInfo.InvariantCulture);

    using var connection = new SqliteConnection(connectionString);

    connection.Open();

    string sql = @"
INSERT INTO MedicoesServidor
    (GatewayId, SensorId, Zona, TimestampMedicao, Tipo, Valor, Unidade)
VALUES
    (@GatewayId, @SensorId, @Zona, @TimestampMedicao, @Tipo, @Valor, @Unidade)";

    using var cmd = new SqliteCommand(sql, connection);

    cmd.Parameters.AddWithValue("@GatewayId", gatewayId);
    cmd.Parameters.AddWithValue("@SensorId", sensorId);
    cmd.Parameters.AddWithValue("@Zona", zona);
    cmd.Parameters.AddWithValue("@TimestampMedicao", timestamp);
    cmd.Parameters.AddWithValue("@Tipo", tipo);
    cmd.Parameters.AddWithValue("@Valor", valor);
    cmd.Parameters.AddWithValue("@Unidade", unidade);

    cmd.ExecuteNonQuery();
}

class AnalysisResponse
{
    public string Tipo { get; set; } = "";
    public double Valor { get; set; }
    public string Resultado { get; set; } = "";
}