using System;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

Console.OutputEncoding = Encoding.UTF8;

const int serverPort = 5000;
const string dbName = "OneHealthDB";
const string masterConnectionString = @"Server=(localdb)\MSSQLLocalDB;Integrated Security=true;TrustServerCertificate=true;";
string dbConnectionString = $@"Server=(localdb)\MSSQLLocalDB;Database={dbName};Integrated Security=true;TrustServerCertificate=true;";

// Objeto de exclusão mútua para o ficheiro
object fileLock = new object();

InicializarBaseDeDados(masterConnectionString, dbName, dbConnectionString);

TcpListener listener = new TcpListener(IPAddress.Loopback, serverPort);
listener.Start();

Console.WriteLine($"[SERVIDOR] À escuta na porta {serverPort}...");

while (true)
{
    TcpClient gatewayClient = listener.AcceptTcpClient();
    Console.WriteLine("[SERVIDOR] Gateway ligado.");

    _ = Task.Run(() => HandleGateway(gatewayClient, dbConnectionString, fileLock));
}

static void HandleGateway(TcpClient client, string dbConnectionString, object fileLock)
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

                            lock (fileLock)
                            {
                                File.AppendAllText("dados_servidor.txt", logLine + Environment.NewLine);
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

                            Console.WriteLine($"[SERVIDOR] Guardado: {logLine}");
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

static void InicializarBaseDeDados(string masterConnectionString, string dbName, string dbConnectionString)
{
    using (var connection = new SqlConnection(masterConnectionString))
    {
        connection.Open();

        string createDbSql = $@"
IF DB_ID('{dbName}') IS NULL
BEGIN
    CREATE DATABASE [{dbName}]
END";

        using var cmd = new SqlCommand(createDbSql, connection);
        cmd.ExecuteNonQuery();
    }

    using (var connection = new SqlConnection(dbConnectionString))
    {
        connection.Open();

        string createTablesSql = @"
IF OBJECT_ID('MedicoesServidor', 'U') IS NULL
BEGIN
    CREATE TABLE MedicoesServidor (
        Id INT IDENTITY(1,1) PRIMARY KEY,
        GatewayId NVARCHAR(50) NOT NULL,
        SensorId NVARCHAR(50) NOT NULL,
        Zona NVARCHAR(100) NOT NULL,
        TimestampMedicao DATETIME NOT NULL,
        Tipo NVARCHAR(50) NOT NULL,
        Valor FLOAT NOT NULL,
        Unidade NVARCHAR(50) NOT NULL
    )
END";

        using var cmd = new SqlCommand(createTablesSql, connection);
        cmd.ExecuteNonQuery();
    }
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

    using var connection = new SqlConnection(connectionString);
    connection.Open();

    string sql = @"
INSERT INTO MedicoesServidor
    (GatewayId, SensorId, Zona, TimestampMedicao, Tipo, Valor, Unidade)
VALUES
    (@GatewayId, @SensorId, @Zona, @TimestampMedicao, @Tipo, @Valor, @Unidade)";

    using var cmd = new SqlCommand(sql, connection);
    cmd.Parameters.AddWithValue("@GatewayId", gatewayId);
    cmd.Parameters.AddWithValue("@SensorId", sensorId);
    cmd.Parameters.AddWithValue("@Zona", zona);
    cmd.Parameters.AddWithValue("@TimestampMedicao", DateTime.Parse(timestamp));
    cmd.Parameters.AddWithValue("@Tipo", tipo);
    cmd.Parameters.AddWithValue("@Valor", valor);
    cmd.Parameters.AddWithValue("@Unidade", unidade);

    cmd.ExecuteNonQuery();
}