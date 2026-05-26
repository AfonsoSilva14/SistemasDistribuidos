using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net.Http.Json;
using System.Net.Sockets;
using System.Text;
using Microsoft.Data.Sqlite;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

Console.OutputEncoding = Encoding.UTF8;

const string gatewayId = "GW01";
const string serverIp = "127.0.0.1";
const int serverPort = 5000;
const int heartbeatTimeoutSeconds = 60;

const string sensoresFile = "sensores.txt";
const string sensoresConfigFile = "sensores_config.csv";
const string dadosRecebidosFile = "dados_recebidos.txt";
const string agregadoFile = "agregado.txt";

const string exchangeName = "sensores_exchange";
const string queueName = "gateway_gw01_queue";

string dbConnectionString = "Data Source=gateway.db";

object sensorDbLock = new object();
object sensoresFileLock = new object();
object dadosRecebidosFileLock = new object();
object agregadoFileLock = new object();
object serverConnectionLock = new object();

var sensorDb = CarregarSensoresConfigurados(sensoresConfigFile);

lock (sensoresFileLock)
{
    GuardarSensores(sensorDb, sensoresFile);
}

InicializarBaseDeDados(dbConnectionString);

lock (sensorDbLock)
{
    GuardarSensoresNaBaseDeDados(sensorDb, dbConnectionString);
}

// Ligação Gateway → Servidor por TCP
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

// Verificação de sensores sem heartbeat
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
                        Console.WriteLine($"[GATEWAY] Sensor {sensor.SensorId} sem heartbeat há {elapsed:F0}s → manutenção.");
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

// RabbitMQ
var factory = new ConnectionFactory()
{
    HostName = "localhost"
};

using var rabbitConnection = factory.CreateConnection();
using var channel = rabbitConnection.CreateModel();

channel.ExchangeDeclare(
    exchange: exchangeName,
    type: ExchangeType.Topic,
    durable: true,
    autoDelete: false);

channel.QueueDeclare(
    queue: queueName,
    durable: true,
    exclusive: false,
    autoDelete: false);

var zonasSubscritas = sensorDb.Values
    .Select(sensor => sensor.Zona.ToLowerInvariant())
    .Distinct(StringComparer.OrdinalIgnoreCase)
    .ToList();

foreach (string zonaSubscrita in zonasSubscritas)
{
    channel.QueueBind(
        queue: queueName,
        exchange: exchangeName,
        routingKey: $"{zonaSubscrita}.#");
}

var consumer = new EventingBasicConsumer(channel);

consumer.Received += async (model, ea) =>
{
    string message = Encoding.UTF8.GetString(ea.Body.ToArray());

    Console.WriteLine($"[GATEWAY] Recebido via RabbitMQ: {message}");

    try
    {
        await ProcessarMensagemSensor(
            message,
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
            serverConnectionLock);

        channel.BasicAck(ea.DeliveryTag, false);
    }
    catch (Exception ex)
    {
        Console.WriteLine("[GATEWAY] Erro ao processar mensagem: " + ex.Message);
        channel.BasicNack(ea.DeliveryTag, false, true);
    }
};

channel.BasicConsume(
    queue: queueName,
    autoAck: false,
    consumer: consumer);

Console.WriteLine("[GATEWAY] A consumir mensagens RabbitMQ.");
Console.WriteLine("[GATEWAY] Topicos subscritos: " +
                  string.Join(", ", zonasSubscritas.Select(z => $"{z}.#")));
Console.WriteLine("[GATEWAY] Pressiona ENTER para terminar.");
Console.ReadLine();

static Dictionary<string, SensorInfo> CarregarSensoresConfigurados(string sensoresConfigFile)
{
    CriarConfigSensoresPorDefeito(sensoresConfigFile);

    var sensores = new Dictionary<string, SensorInfo>(StringComparer.OrdinalIgnoreCase);

    foreach (string linha in File.ReadAllLines(sensoresConfigFile))
    {
        string trimmed = linha.Trim();
        if (trimmed.Length == 0 || trimmed.StartsWith("#"))
            continue;

        string[] parts = trimmed.Split(';', StringSplitOptions.TrimEntries);
        if (parts.Length < 4)
        {
            Console.WriteLine($"[GATEWAY] Linha ignorada em {sensoresConfigFile}: {linha}");
            continue;
        }

        string sensorId = parts[0];
        string zona = parts[1];
        string estado = parts[2];
        string[] tipos = parts[3].Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (sensorId.Length == 0 || zona.Length == 0 || tipos.Length == 0)
        {
            Console.WriteLine($"[GATEWAY] Linha incompleta ignorada em {sensoresConfigFile}: {linha}");
            continue;
        }

        sensores[sensorId] = new SensorInfo
        {
            SensorId = sensorId,
            Estado = estado.Length == 0 ? "ativo" : estado,
            Zona = zona,
            TiposSuportados = new HashSet<string>(
                tipos.Select(NormalizarTipo),
                StringComparer.OrdinalIgnoreCase),
            LastSync = DateTime.Now
        };
    }

    if (sensores.Count == 0)
    {
        throw new InvalidOperationException(
            $"Nao existem sensores validos em {sensoresConfigFile}.");
    }

    Console.WriteLine($"[GATEWAY] Sensores carregados: {sensores.Count}");
    return sensores;
}

static void CriarConfigSensoresPorDefeito(string sensoresConfigFile)
{
    if (File.Exists(sensoresConfigFile))
        return;

    var linhas = new[]
    {
        "# SensorId;Zona;Estado;TiposSuportados",
        "S102;ZONA_ESCOLAR;ativo;TEMP,PM25,PM10,HUM,CO2,PRESS,RUIDO,AR,LUZ",
        "S103;ZONA_ESCOLAR;ativo;TEMP,HUM,CO2",
        "S201;ZONA_CENTRO;ativo;PM25,PM10,RUIDO,AR",
        "S301;ZONA_PARQUE;ativo;TEMP,HUM,LUZ"
    };

    File.WriteAllLines(sensoresConfigFile, linhas);
    Console.WriteLine($"[GATEWAY] Criado ficheiro de configuracao: {sensoresConfigFile}");
}

static async Task ProcessarMensagemSensor(
    string line,
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
    string[] parts = line.Split('|');
    string command = parts[0];

    switch (command)
    {
        case "DATA":
            {
                if (parts.Length < 7)
                {
                    Console.WriteLine("[GATEWAY] DATA_NACK|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                    break;
                }

                string sensorId = parts[1];
                string zonaRecebida = parts[2];
                string timestamp = parts[3];
                string tipo = NormalizarTipo(parts[4].Trim());
                string valorTexto = parts[5].Trim();
                string unidade = parts[6].Trim();

                if (!sensorDb.ContainsKey(sensorId))
                {
                    Console.WriteLine($"[GATEWAY] DATA_NACK|{sensorId}|{timestamp}|SENSOR_NOT_REGISTERED");
                    break;
                }

                string zonaSensor;

                lock (sensorDbLock)
                {
                    SensorInfo sensor = sensorDb[sensorId];

                    if (!sensor.Estado.Equals("ativo", StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"[GATEWAY] DATA_NACK|{sensorId}|{timestamp}|SENSOR_NOT_ACTIVE");
                        break;
                    }

                    if (!sensor.Zona.Equals(zonaRecebida, StringComparison.OrdinalIgnoreCase))
                    {
                        Console.WriteLine($"[GATEWAY] DATA_NACK|{sensorId}|{timestamp}|INVALID_ZONE");
                        break;
                    }

                    if (!sensor.TiposSuportados.Contains(tipo))
                    {
                        Console.WriteLine($"[GATEWAY] DATA_NACK|{sensorId}|{timestamp}|UNSUPPORTED_TYPE");
                        break;
                    }

                    sensor.LastSync = DateTime.Now;
                    zonaSensor = sensor.Zona;
                }

                if (!TentarLerDouble(valorTexto, out double valor))
                {
                    Console.WriteLine($"[GATEWAY] DATA_NACK|{sensorId}|{timestamp}|INVALID_VALUE");
                    break;
                }

                if (!UnidadeValida(tipo, unidade))
                {
                    Console.WriteLine($"[GATEWAY] DATA_NACK|{sensorId}|{timestamp}|INVALID_UNIT");
                    break;
                }

                // RPC → PreProcessingService
                PreProcessResponse pre = await ChamarPreProcessingService(sensorId, tipo, valor, unidade);

                tipo = pre.Tipo;
                valor = pre.Valor;
                unidade = pre.Unidade;

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

                Console.WriteLine($"[GATEWAY] DATA_ACK|{sensorId}|{timestamp}|OK");

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
                if (parts.Length < 4)
                {
                    Console.WriteLine("[GATEWAY] HEARTBEAT_ACK|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                    break;
                }

                string sensorId = parts[1];
                string zonaRecebida = parts[2];
                string timestamp = parts[3];

                lock (sensorDbLock)
                {
                    if (sensorDb.ContainsKey(sensorId) &&
                        sensorDb[sensorId].Zona.Equals(zonaRecebida, StringComparison.OrdinalIgnoreCase))
                    {
                        sensorDb[sensorId].LastSync = DateTime.Now;
                    }
                }

                lock (sensoresFileLock)
                {
                    GuardarSensores(sensorDb, sensoresFile);
                }

                lock (sensorDbLock)
                {
                    GuardarSensoresNaBaseDeDados(sensorDb, dbConnectionString);
                }

                Console.WriteLine($"[GATEWAY] HEARTBEAT_ACK|{sensorId}|{timestamp}|OK");
                break;
            }

        case "BYE":
            {
                if (parts.Length < 4)
                {
                    Console.WriteLine("[GATEWAY] BYE_ACK|UNKNOWN|UNKNOWN|INVALID_FORMAT");
                    break;
                }

                string sensorId = parts[1];
                string zona = parts[2];
                string timestamp = parts[3];

                Console.WriteLine($"[GATEWAY] BYE_ACK|{sensorId}|{zona}|{timestamp}|OK");
                break;
            }

        default:
            {
                Console.WriteLine("[GATEWAY] ERROR|UNKNOWN_COMMAND");
                break;
            }
    }
}

static async Task<PreProcessResponse> ChamarPreProcessingService(
    string sensorId,
    string tipo,
    double valor,
    string unidade)
{
    try
    {
        using HttpClient http = new HttpClient();

        var pedido = new
        {
            SensorId = sensorId,
            Tipo = tipo,
            Valor = valor,
            Unidade = unidade
        };

        var resposta = await http.PostAsJsonAsync(
            "http://localhost:7001/preprocess",
            pedido);

        if (!resposta.IsSuccessStatusCode)
        {
            return new PreProcessResponse
            {
                SensorId = sensorId,
                Tipo = tipo,
                Valor = Math.Round(valor, 2),
                Unidade = unidade
            };
        }

        var resultado = await resposta.Content.ReadFromJsonAsync<PreProcessResponse>();

        return resultado ?? new PreProcessResponse
        {
            SensorId = sensorId,
            Tipo = tipo,
            Valor = Math.Round(valor, 2),
            Unidade = unidade
        };
    }
    catch
    {
        return new PreProcessResponse
        {
            SensorId = sensorId,
            Tipo = tipo,
            Valor = Math.Round(valor, 2),
            Unidade = unidade
        };
    }
}

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
    string t = tipo.ToUpperInvariant();
    string u = NormalizarUnidade(unidade);

    if (t == "TEMP" && (u is "c" or "oc" or "f" or "of" or "k"))
        return true;
    if ((t == "PM25" || t == "PM10" || t == "AR") && (u is "ug/m3" or "mg/m3" or "aqi"))
        return true;
    if (t == "PRESS" && (u is "hpa" or "pa" or "kpa"))
        return true;

    return tipo.ToUpperInvariant() switch
    {
        "TEMP" => unidade.Equals("C", StringComparison.OrdinalIgnoreCase) ||
                  unidade.Equals("ºC", StringComparison.OrdinalIgnoreCase),
        "PM25" => unidade.Equals("ug/m3", StringComparison.OrdinalIgnoreCase) ||
                  unidade.Equals("µg/m3", StringComparison.OrdinalIgnoreCase),
        "PM10" => unidade.Equals("ug/m3", StringComparison.OrdinalIgnoreCase) ||
                  unidade.Equals("µg/m3", StringComparison.OrdinalIgnoreCase),
        "HUM" => unidade.Equals("%", StringComparison.OrdinalIgnoreCase),
        "CO2" => unidade.Equals("ppm", StringComparison.OrdinalIgnoreCase),
        "PRESS" => unidade.Equals("hPa", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("Pa", StringComparison.OrdinalIgnoreCase),
        "RUIDO" => unidade.Equals("dB", StringComparison.OrdinalIgnoreCase) ||
                   unidade.Equals("dBA", StringComparison.OrdinalIgnoreCase),
        "AR" => unidade.Equals("AQI", StringComparison.OrdinalIgnoreCase) ||
                unidade.Equals("ug/m3", StringComparison.OrdinalIgnoreCase) ||
                unidade.Equals("µg/m3", StringComparison.OrdinalIgnoreCase),
        "LUZ" => unidade.Equals("lux", StringComparison.OrdinalIgnoreCase) ||
                 unidade.Equals("lx", StringComparison.OrdinalIgnoreCase),
        _ => false
    };
}

static string NormalizarUnidade(string unidade)
{
    return unidade
        .Trim()
        .ToLowerInvariant()
        .Replace("Â", "")
        .Replace("º", "o")
        .Replace("°", "o")
        .Replace("µ", "u")
        .Replace("μ", "u");
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

class PreProcessResponse
{
    public string SensorId { get; set; } = "";
    public string Tipo { get; set; } = "";
    public double Valor { get; set; }
    public string Unidade { get; set; } = "";
}
