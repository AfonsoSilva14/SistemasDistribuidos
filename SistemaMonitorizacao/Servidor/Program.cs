using System;
using System.Collections.Concurrent;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

Console.OutputEncoding = Encoding.UTF8;

const int serverPort = 5000;
string dbConnectionString = "Data Source=servidor.db";

// Um lock por tipo de dado → acesso sequencial por ficheiro (protocolo exige mutex por ficheiro)
var fileLocks = new ConcurrentDictionary<string, object>(StringComparer.OrdinalIgnoreCase);

InicializarBaseDeDados(dbConnectionString);

TcpListener listener = new TcpListener(IPAddress.Loopback, serverPort);
listener.Start();

Console.WriteLine($"[SERVIDOR] À escuta na porta {serverPort}...");

while (true)
{
    TcpClient gatewayClient = listener.AcceptTcpClient();
    Console.WriteLine("[SERVIDOR] Gateway ligado.");

    _ = Task.Run(() => HandleGateway(gatewayClient, dbConnectionString, fileLocks));
}

static void HandleGateway(TcpClient client, string dbConnectionString, ConcurrentDictionary<string, object> fileLocks)
{
    try
    {
        using NetworkStream ns = client.GetStream();
        using StreamReader reader = new StreamReader(ns, Encoding.UTF8);
        using StreamWriter writer = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };

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

                            string logLine = $"{timestamp}|{gatewayId}|{sensorId}|{zona}|{tipo}|{valor}|{unidade}";

                            // Guarda em ficheiro específico do tipo de dado
                            string tipoFicheiro = $"dados_{tipo.ToUpperInvariant()}.txt";
                            var lockObj = fileLocks.GetOrAdd(tipo, _ => new object());
                            lock (lockObj)
                            {
                                File.AppendAllText(tipoFicheiro, logLine + Environment.NewLine);
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

                            Console.WriteLine($"[SERVIDOR] Guardado em {tipoFicheiro}: {logLine}");
                            writer.WriteLine($"SERVER_ACK|{gatewayId}|{sensorId}|{timestamp}|OK");
                        }
                        else
                        {
                            writer.WriteLine("SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                        }
                        break;
                    }

                case "GW_BYE":
                    {
                        if (parts.Length >= 3)
                        {
                            string gatewayId = parts[1];
                            string timestamp = parts[2];
                            writer.WriteLine($"GW_BYE_ACK|{gatewayId}|{timestamp}|OK");
                        }
                        else
                        {
                            writer.WriteLine("SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                        }

                        Console.WriteLine("[SERVIDOR] Gateway terminou comunicação.");
                        break;
                    }

                case "GW_HEARTBEAT":
                    {
                        if (parts.Length >= 3)
                        {
                            string gatewayId = parts[1];
                            string timestamp = parts[2];
                            writer.WriteLine($"GW_HEARTBEAT_ACK|{gatewayId}|{timestamp}|OK");
                        }
                        else
                        {
                            writer.WriteLine("SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                        }
                        break;
                    }

                default:
                    writer.WriteLine("SERVER_NACK|UNKNOWN|UNKNOWN|UNKNOWN|UNKNOWN_COMMAND");
                    break;
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
    using var cmd = new SqliteCommand(createMedicoes, connection);
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
    double valor = double.Parse(valorTexto, CultureInfo.InvariantCulture);

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
