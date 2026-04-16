using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Data.Sqlite;

Console.OutputEncoding = Encoding.UTF8;

const string gatewayId = "GW01";
const string serverIp = "127.0.0.1";
const int serverPort = 5000;
const int gatewayPort = 6000;
const int heartbeatTimeoutSeconds = 60; // sensor sem heartbeat há 60s → manutenção

const string sensoresFile = "sensores.txt";
const string dadosRecebidosFile = "dados_recebidos.txt";
const string agregadoFile = "agregado.txt";

string dbConnectionString = "Data Source=gateway.db";

// Locks
object sensorDbLock = new object();
object sensoresFileLock = new object();
object dadosRecebidosFileLock = new object();
object agregadoFileLock = new object();
object serverConnectionLock = new object();

var sensorDb = new Dictionary<string, SensorInfo>(StringComparer.OrdinalIgnoreCase)
{
    ["S102"] = new SensorInfo
    {
        SensorId = "S102",
        Estado = "ativo",
        Zona = "ZONA_ESCOLAR",
        TiposSuportados = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
            { "TEMP", "PM25", "PM2.5", "PM10", "HUM", "CO2", "PRESS", "RUIDO", "AR", "LUZ" },
        LastSync = DateTime.Now
    }
};

lock (sensoresFileLock)
{
    GuardarSensores(sensorDb, sensoresFile);
}

InicializarBaseDeDados(dbConnectionString);

lock (sensorDbLock)
{
    GuardarSensoresNaBaseDeDados(sensorDb, dbConnectionString);
}

TcpClient serverClient = new TcpClient();

try
{
    serverClient.Connect(serverIp, serverPort);
}
catch (SocketException)
{
    Console.WriteLine("[GATEWAY] Não foi possível ligar ao servidor. Confirma se o Servidor está em execução na porta 5000.");
    return;
}

NetworkStream serverNs = serverClient.GetStream();
StreamReader serverReader = new StreamReader(serverNs, Encoding.UTF8);
StreamWriter serverWriter = new StreamWriter(serverNs, Encoding.UTF8) { AutoFlush = true };

string localIp = "127.0.0.1";

lock (serverConnectionLock)
{
    serverWriter.WriteLine($"GW_HELLO|{gatewayId}|{localIp}");
    Console.WriteLine("[GATEWAY] Enviado ao servidor: GW_HELLO");
    Console.WriteLine("[GATEWAY] Resposta do servidor: " + serverReader.ReadLine());
}

// Deteção de sensores sem heartbeat (executa em background a cada 30s)
_ = Task.Run(async () =>
{
    while (true)
    {
        await Task.Delay(TimeSpan.FromSeconds(30));
        var now = DateTime.Now;
        bool changed = false;

        lock (sensorDbLock)
        {
            foreach (var sensor in sensorDb.Values)
            {
                if (sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                {
                    double elapsed = (now - sensor.LastSync).TotalSeconds;
                    if (elapsed > heartbeatTimeoutSeconds)
                    {
                        Console.WriteLine($"[GATEWAY] Sensor {sensor.SensorId} sem heartbeat há {elapsed:F0}s → marcado como manutenção.");
                        sensor.Estado = "manutenção";
                        changed = true;
                    }
                }
            }
        }

        if (changed)
        {
            lock (sensoresFileLock)
            {
                GuardarSensores(sensorDb, sensoresFile);
            }
            lock (sensorDbLock)
            {
                GuardarSensoresNaBaseDeDados(sensorDb, dbConnectionString);
            }
        }
    }
});

TcpListener listener = new TcpListener(IPAddress.Loopback, gatewayPort);
listener.Start();

Console.WriteLine($"[GATEWAY] À escuta de sensores em 127.0.0.1:{gatewayPort}...");

while (true)
{
    TcpClient sensorClient = listener.AcceptTcpClient();
    Console.WriteLine("[GATEWAY] Sensor ligado.");

    _ = Task.Run(() => HandleSensor(
        sensorClient,
        sensorDb,
        gatewayId,
        serverWriter,
        serverReader,
        sensoresFile,
        dadosRecebidosFile,
        agregadoFile,
        dbConnectionString,
        sensorDbLock,
        sensoresFileLock,
        dadosRecebidosFileLock,
        agregadoFileLock,
        serverConnectionLock));
}

static void HandleSensor(
    TcpClient client,
    Dictionary<string, SensorInfo> sensorDb,
    string gatewayId,
    StreamWriter serverWriter,
    StreamReader serverReader,
    string sensoresFile,
    string dadosRecebidosFile,
    string agregadoFile,
    string dbConnectionString,
    object sensorDbLock,
    object sensoresFileLock,
    object dadosRecebidosFileLock,
    object agregadoFileLock,
    object serverConnectionLock)
{
    try
    {
        using NetworkStream ns = client.GetStream();
        using StreamReader reader = new StreamReader(ns, Encoding.UTF8);
        using StreamWriter writer = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };

        string? currentSensorId = null;
        bool authenticated = false;
        bool typesRegistered = false;
        HashSet<string> registeredTypes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        string? line;
        while ((line = reader.ReadLine()) != null)
        {
            Console.WriteLine($"[GATEWAY] Recebido do sensor: {line}");

            string[] parts = line.Split('|');
            string command = parts[0];

            switch (command)
            {
                case "HELLO":
                    {
                        if (parts.Length < 3)
                        {
                            writer.WriteLine("AUTH_FAIL|UNKNOWN|INVALID_FORMAT");
                            break;
                        }

                        string sensorId = parts[1];
                        string zona = parts[2];

                        writer.WriteLine($"HELLO_ACK|{gatewayId}|OK");

                        SensorInfo? sensor;
                        lock (sensorDbLock)
                        {
                            if (!sensorDb.ContainsKey(sensorId))
                            {
                                writer.WriteLine($"AUTH_FAIL|{sensorId}|NOT_REGISTERED");
                                break;
                            }

                            sensor = sensorDb[sensorId];

                            if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                            {
                                string reason =
                                    sensor.Estado.Equals("manutenção", StringComparison.OrdinalIgnoreCase) ||
                                    sensor.Estado.Equals("manutencao", StringComparison.OrdinalIgnoreCase)
                                        ? "MAINTENANCE"
                                        : "DISABLED";

                                writer.WriteLine($"AUTH_FAIL|{sensorId}|{reason}");
                                break;
                            }

                            if (!sensor.Zona.Equals(zona, StringComparison.OrdinalIgnoreCase))
                            {
                                writer.WriteLine($"AUTH_FAIL|{sensorId}|INVALID_ZONE");
                                break;
                            }

                            currentSensorId = sensorId;
                            authenticated = true;
                            sensor.LastSync = DateTime.Now;
                        }

                        lock (sensoresFileLock)
                        {
                            GuardarSensores(sensorDb, sensoresFile);
                        }

                        lock (sensorDbLock)
                        {
                            GuardarSensoresNaBaseDeDados(sensorDb, dbConnectionString);
                        }

                        writer.WriteLine($"AUTH_OK|{sensorId}|ativo|{zona}");
                        break;
                    }

                case "REGISTER_TYPES":
                    {
                        if (!authenticated || currentSensorId == null)
                        {
                            writer.WriteLine("REGISTER_NACK|UNKNOWN|NOT_AUTHENTICATED|NONE");
                            break;
                        }

                        if (parts.Length < 3)
                        {
                            writer.WriteLine($"REGISTER_NACK|{currentSensorId}|INVALID_FORMAT|NONE");
                            break;
                        }

                        string sensorId = parts[1];
                        string[] tipos = parts[2].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

                        if (!sensorId.Equals(currentSensorId, StringComparison.OrdinalIgnoreCase))
                        {
                            writer.WriteLine($"REGISTER_NACK|{sensorId}|SENSOR_MISMATCH|NONE");
                            break;
                        }

                        List<string> invalidos = new();

                        lock (sensorDbLock)
                        {
                            var sensor = sensorDb[currentSensorId];

                            foreach (string tipo in tipos)
                            {
                                string tipoNorm = NormalizarTipo(tipo.Trim());

                                if (sensor.TiposSuportados.Contains(tipoNorm) ||
                                    sensor.TiposSuportados.Contains(tipo.Trim()))
                                    registeredTypes.Add(tipoNorm);
                                else
                                    invalidos.Add(tipoNorm);
                            }
                        }

                        if (invalidos.Count > 0)
                        {
                            writer.WriteLine($"REGISTER_NACK|{sensorId}|INVALID_TYPES|{string.Join(",", invalidos)}");
                            break;
                        }

                        typesRegistered = true;
                        writer.WriteLine($"REGISTER_ACK|{sensorId}|OK|{string.Join(",", registeredTypes)}");
                        break;
                    }

                case "DATA":
                    {
                        if (!authenticated || !typesRegistered || currentSensorId == null)
                        {
                            writer.WriteLine("DATA_NACK|UNKNOWN|UNKNOWN|NOT_READY");
                            break;
                        }

                        if (parts.Length < 6)
                        {
                            writer.WriteLine($"DATA_NACK|{currentSensorId}|UNKNOWN|INVALID_FORMAT");
                            break;
                        }

                        string sensorId = parts[1];
                        string timestamp = parts[2];
                        string tipo = NormalizarTipo(parts[3].Trim());
                        string valorTexto = parts[4].Trim();
                        string unidade = parts[5].Trim();

                        if (!sensorId.Equals(currentSensorId, StringComparison.OrdinalIgnoreCase))
                        {
                            writer.WriteLine($"DATA_NACK|{sensorId}|{timestamp}|SENSOR_MISMATCH");
                            break;
                        }

                        if (!registeredTypes.Contains(tipo))
                        {
                            writer.WriteLine($"DATA_NACK|{sensorId}|{timestamp}|UNSUPPORTED_TYPE");
                            break;
                        }

                        if (!TentarLerDouble(valorTexto, out double valor))
                        {
                            writer.WriteLine($"DATA_NACK|{sensorId}|{timestamp}|INVALID_VALUE");
                            break;
                        }

                        if (!UnidadeValida(tipo, unidade))
                        {
                            writer.WriteLine($"DATA_NACK|{sensorId}|{timestamp}|INVALID_UNIT");
                            break;
                        }

                        valor = Math.Round(valor, 2);

                        string zonaSensor;

                        lock (sensorDbLock)
                        {
                            SensorInfo sensor = sensorDb[currentSensorId];
                            sensor.LastSync = DateTime.Now;
                            zonaSensor = sensor.Zona;
                        }

                        lock (sensoresFileLock)
                        {
                            GuardarSensores(sensorDb, sensoresFile);
                        }

                        lock (sensorDbLock)
                        {
                            GuardarSensoresNaBaseDeDados(sensorDb, dbConnectionString);
                        }

                        string linhaDado =
                            $"{timestamp}|{sensorId}|{zonaSensor}|{tipo}|{valor.ToString("F2", CultureInfo.InvariantCulture)}|{unidade}";

                        lock (dadosRecebidosFileLock)
                        {
                            File.AppendAllText(dadosRecebidosFile, linhaDado + Environment.NewLine);
                        }

                        lock (agregadoFileLock)
                        {
                            AtualizarAgregado(agregadoFile, sensorId, tipo, valor);
                        }

                        GuardarMedicaoGatewayNaBaseDeDados(
                            dbConnectionString,
                            timestamp,
                            gatewayId,
                            sensorId,
                            zonaSensor,
                            tipo,
                            valor,
                            unidade);

                        writer.WriteLine($"DATA_ACK|{sensorId}|{timestamp}|OK");

                        string gwData =
                            $"GW_DATA|{gatewayId}|{sensorId}|{zonaSensor}|{timestamp}|{tipo}|{valor.ToString("F2", CultureInfo.InvariantCulture)}|{unidade}";

                        string? serverResponse;
                        lock (serverConnectionLock)
                        {
                            serverWriter.WriteLine(gwData);
                            serverResponse = serverReader.ReadLine();
                        }

                        Console.WriteLine("[GATEWAY] Resposta do servidor: " + serverResponse);
                        break;
                    }

                case "HEARTBEAT":
                    {
                        if (!authenticated || currentSensorId == null)
                        {
                            writer.WriteLine("HEARTBEAT_ACK|UNKNOWN|UNKNOWN|NOT_AUTHENTICATED");
                            break;
                        }

                        if (parts.Length < 3)
                        {
                            writer.WriteLine($"HEARTBEAT_ACK|{currentSensorId}|UNKNOWN|INVALID_FORMAT");
                            break;
                        }

                        string timestamp = parts[2];

                        lock (sensorDbLock)
                        {
                            sensorDb[currentSensorId].LastSync = DateTime.Now;
                        }

                        lock (sensoresFileLock)
                        {
                            GuardarSensores(sensorDb, sensoresFile);
                        }

                        lock (sensorDbLock)
                        {
                            GuardarSensoresNaBaseDeDados(sensorDb, dbConnectionString);
                        }

                        writer.WriteLine($"HEARTBEAT_ACK|{currentSensorId}|{timestamp}|OK");
                        break;
                    }

                case "BYE":
                    {
                        if (parts.Length < 3)
                        {
                            writer.WriteLine("BYE_ACK|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                            break;
                        }

                        string sensorId = parts[1];
                        string timestamp = parts[2];

                        writer.WriteLine($"BYE_ACK|{sensorId}|{timestamp}|OK");
                        Console.WriteLine("[GATEWAY] Sensor terminou comunicação.");
                        return;
                    }

                default:
                    writer.WriteLine("ERROR|UNKNOWN_COMMAND");
                    break;
            }
        }
    }
    catch (Exception ex)
    {
        Console.WriteLine($"[GATEWAY] Erro: {ex.Message}");
    }
    finally
    {
        client.Close();
        Console.WriteLine("[GATEWAY] Ligação ao sensor fechada.");
    }
}

// Normaliza nomes de tipos: PM2.5 → PM25
static string NormalizarTipo(string tipo)
{
    return tipo.ToUpperInvariant() switch
    {
        "PM2.5" => "PM25",
        _ => tipo.ToUpperInvariant()
    };
}

static bool TentarLerDouble(string texto, out double valor)
{
    return double.TryParse(texto, NumberStyles.Any, CultureInfo.InvariantCulture, out valor) ||
           double.TryParse(texto, NumberStyles.Any, new CultureInfo("pt-PT"), out valor);
}

static bool UnidadeValida(string tipo, string unidade)
{
    return tipo.ToUpperInvariant() switch
    {
        "TEMP"  => unidade.Equals("C", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("ºC", StringComparison.OrdinalIgnoreCase),
        "PM25"  => unidade.Equals("ug/m3", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("µg/m3", StringComparison.OrdinalIgnoreCase),
        "PM10"  => unidade.Equals("ug/m3", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("µg/m3", StringComparison.OrdinalIgnoreCase),
        "HUM"   => unidade.Equals("%", StringComparison.OrdinalIgnoreCase),
        "CO2"   => unidade.Equals("ppm", StringComparison.OrdinalIgnoreCase),
        "PRESS" => unidade.Equals("hPa", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("Pa", StringComparison.OrdinalIgnoreCase),
        "RUIDO" => unidade.Equals("dB", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("dBA", StringComparison.OrdinalIgnoreCase),
        "AR"    => unidade.Equals("AQI", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("ug/m3", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("µg/m3", StringComparison.OrdinalIgnoreCase),
        "LUZ"   => unidade.Equals("lux", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("lx", StringComparison.OrdinalIgnoreCase),
        _ => false
    };
}

static void GuardarSensores(Dictionary<string, SensorInfo> sensorDb, string sensoresFile)
{
    List<string> linhas = new();

    foreach (var sensor in sensorDb.Values)
    {
        string tipos = string.Join(",", sensor.TiposSuportados);
        string linha =
            $"{sensor.SensorId};{sensor.Estado};{sensor.Zona};{tipos};{sensor.LastSync:yyyy-MM-ddTHH:mm:ss}";
        linhas.Add(linha);
    }

    File.WriteAllLines(sensoresFile, linhas);
}

static void AtualizarAgregado(string agregadoFile, string sensorId, string tipo, double novoValor)
{
    var agregados = new Dictionary<string, (int Quantidade, double Soma)>(StringComparer.OrdinalIgnoreCase);

    if (File.Exists(agregadoFile))
    {
        string[] linhas = File.ReadAllLines(agregadoFile);

        foreach (string linha in linhas)
        {
            string[] parts = linha.Split('|');
            if (parts.Length == 4)
            {
                string sId = parts[0];
                string t = parts[1];
                int quantidade = int.Parse(parts[2]);
                double media = double.Parse(parts[3], CultureInfo.InvariantCulture);

                agregados[$"{sId}|{t}"] = (quantidade, media * quantidade);
            }
        }
    }

    string chave = $"{sensorId}|{tipo}";

    if (agregados.ContainsKey(chave))
    {
        var atual = agregados[chave];
        agregados[chave] = (atual.Quantidade + 1, atual.Soma + novoValor);
    }
    else
    {
        agregados[chave] = (1, novoValor);
    }

    List<string> novasLinhas = new();

    foreach (var item in agregados)
    {
        string[] partes = item.Key.Split('|');
        string sId = partes[0];
        string t = partes[1];
        int quantidade = item.Value.Quantidade;
        double media = item.Value.Soma / quantidade;

        novasLinhas.Add($"{sId}|{t}|{quantidade}|{media.ToString("F2", CultureInfo.InvariantCulture)}");
    }

    File.WriteAllLines(agregadoFile, novasLinhas);
}

static void InicializarBaseDeDados(string dbConnectionString)
{
    using var connection = new SqliteConnection(dbConnectionString);
    connection.Open();

    string createSensores = @"
CREATE TABLE IF NOT EXISTS Sensores (
    SensorId TEXT PRIMARY KEY,
    Estado TEXT NOT NULL,
    Zona TEXT NOT NULL,
    TiposSuportados TEXT NOT NULL,
    LastSync TEXT NOT NULL
)";
    using (var cmd = new SqliteCommand(createSensores, connection))
        cmd.ExecuteNonQuery();

    string createMedicoes = @"
CREATE TABLE IF NOT EXISTS MedicoesGateway (
    Id INTEGER PRIMARY KEY AUTOINCREMENT,
    TimestampMedicao TEXT NOT NULL,
    GatewayId TEXT NOT NULL,
    SensorId TEXT NOT NULL,
    Zona TEXT NOT NULL,
    Tipo TEXT NOT NULL,
    Valor REAL NOT NULL,
    Unidade TEXT NOT NULL
)";
    using (var cmd = new SqliteCommand(createMedicoes, connection))
        cmd.ExecuteNonQuery();
}

static void GuardarSensoresNaBaseDeDados(Dictionary<string, SensorInfo> sensorDb, string connectionString)
{
    using var connection = new SqliteConnection(connectionString);
    connection.Open();

    foreach (var sensor in sensorDb.Values)
    {
        string sql = @"
INSERT OR REPLACE INTO Sensores (SensorId, Estado, Zona, TiposSuportados, LastSync)
VALUES (@SensorId, @Estado, @Zona, @TiposSuportados, @LastSync)";

        using var cmd = new SqliteCommand(sql, connection);
        cmd.Parameters.AddWithValue("@SensorId", sensor.SensorId);
        cmd.Parameters.AddWithValue("@Estado", sensor.Estado);
        cmd.Parameters.AddWithValue("@Zona", sensor.Zona);
        cmd.Parameters.AddWithValue("@TiposSuportados", string.Join(",", sensor.TiposSuportados));
        cmd.Parameters.AddWithValue("@LastSync", sensor.LastSync.ToString("yyyy-MM-ddTHH:mm:ss"));

        cmd.ExecuteNonQuery();
    }
}

static void GuardarMedicaoGatewayNaBaseDeDados(
    string connectionString,
    string timestamp,
    string gatewayId,
    string sensorId,
    string zona,
    string tipo,
    double valor,
    string unidade)
{
    using var connection = new SqliteConnection(connectionString);
    connection.Open();

    string sql = @"
INSERT INTO MedicoesGateway
    (TimestampMedicao, GatewayId, SensorId, Zona, Tipo, Valor, Unidade)
VALUES
    (@TimestampMedicao, @GatewayId, @SensorId, @Zona, @Tipo, @Valor, @Unidade)";

    using var cmd = new SqliteCommand(sql, connection);
    cmd.Parameters.AddWithValue("@TimestampMedicao", timestamp);
    cmd.Parameters.AddWithValue("@GatewayId", gatewayId);
    cmd.Parameters.AddWithValue("@SensorId", sensorId);
    cmd.Parameters.AddWithValue("@Zona", zona);
    cmd.Parameters.AddWithValue("@Tipo", tipo);
    cmd.Parameters.AddWithValue("@Valor", valor);
    cmd.Parameters.AddWithValue("@Unidade", unidade);

    cmd.ExecuteNonQuery();
}

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public HashSet<string> TiposSuportados { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public DateTime LastSync { get; set; }
}
