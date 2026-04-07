using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

Console.OutputEncoding = Encoding.UTF8;

const string gatewayId = "GW01";
const string serverIp = "127.0.0.1";
const int serverPort = 5000;
const int gatewayPort = 6000;

const string sensoresFile = "sensores.txt";
const string dadosRecebidosFile = "dados_recebidos.txt";
const string agregadoFile = "agregado.txt";

// Base simples de sensores registados
var sensorDb = new Dictionary<string, SensorInfo>(StringComparer.OrdinalIgnoreCase)
{
    ["S102"] = new SensorInfo
    {
        SensorId = "S102",
        Estado = "ativo",
        Zona = "ZONA_ESCOLAR",
        TiposSuportados = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "TEMP", "PM25" },
        LastSync = DateTime.Now
    }
};

// Garante que o ficheiro de sensores existe logo ao arrancar
GuardarSensores(sensorDb, sensoresFile);

// Liga ao servidor
TcpClient serverClient = new TcpClient();
serverClient.Connect(serverIp, serverPort);

NetworkStream serverNs = serverClient.GetStream();
StreamReader serverReader = new StreamReader(serverNs, Encoding.UTF8);
StreamWriter serverWriter = new StreamWriter(serverNs, Encoding.UTF8) { AutoFlush = true };

string localIp = "127.0.0.1";
serverWriter.WriteLine($"GW_HELLO|{gatewayId}|{localIp}");
Console.WriteLine("[GATEWAY] Enviado ao servidor: GW_HELLO");
Console.WriteLine("[GATEWAY] Resposta do servidor: " + serverReader.ReadLine());

// Fica à escuta local para sensores
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
        agregadoFile));
}

static void HandleSensor(
    TcpClient client,
    Dictionary<string, SensorInfo> sensorDb,
    string gatewayId,
    StreamWriter serverWriter,
    StreamReader serverReader,
    string sensoresFile,
    string dadosRecebidosFile,
    string agregadoFile)
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

                        if (!sensorDb.ContainsKey(sensorId))
                        {
                            writer.WriteLine($"AUTH_FAIL|{sensorId}|NOT_REGISTERED");
                            break;
                        }

                        var sensor = sensorDb[sensorId];

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

                        GuardarSensores(sensorDb, sensoresFile);

                        writer.WriteLine($"AUTH_OK|{sensorId}|{sensor.Estado}|{sensor.Zona}");
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

                        var sensor = sensorDb[currentSensorId];
                        List<string> invalidos = new();

                        foreach (string tipo in tipos)
                        {
                            string tipoNormalizado = tipo.Trim().ToUpperInvariant();

                            if (sensor.TiposSuportados.Contains(tipoNormalizado))
                                registeredTypes.Add(tipoNormalizado);
                            else
                                invalidos.Add(tipoNormalizado);
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
                        string tipo = parts[3].Trim().ToUpperInvariant();
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

                        // Pré-processamento do valor
                        if (!TentarLerDouble(valorTexto, out double valor))
                        {
                            writer.WriteLine($"DATA_NACK|{sensorId}|{timestamp}|INVALID_VALUE");
                            break;
                        }

                        valor = Math.Round(valor, 2);

                        SensorInfo sensor = sensorDb[currentSensorId];
                        sensor.LastSync = DateTime.Now;

                        // Atualiza ficheiro de sensores
                        GuardarSensores(sensorDb, sensoresFile);

                        // Guarda dado recebido já tratado
                        string linhaDado =
                            $"{timestamp}|{sensorId}|{sensor.Zona}|{tipo}|{valor.ToString("F2", CultureInfo.InvariantCulture)}|{unidade}";
                        File.AppendAllText(dadosRecebidosFile, linhaDado + Environment.NewLine);

                        // Atualiza agregado
                        AtualizarAgregado(agregadoFile, sensorId, tipo, valor);

                        // Responde ao sensor
                        writer.WriteLine($"DATA_ACK|{sensorId}|{timestamp}|OK");

                        // Encaminha ao servidor
                        string gwData =
                            $"GW_DATA|{gatewayId}|{sensorId}|{sensor.Zona}|{timestamp}|{tipo}|{valor.ToString("F2", CultureInfo.InvariantCulture)}|{unidade}";

                        lock (serverWriter)
                        {
                            serverWriter.WriteLine(gwData);
                        }

                        string? serverResponse;
                        lock (serverReader)
                        {
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
                        sensorDb[currentSensorId].LastSync = DateTime.Now;
                        GuardarSensores(sensorDb, sensoresFile);

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

static bool TentarLerDouble(string texto, out double valor)
{
    return double.TryParse(texto, NumberStyles.Any, CultureInfo.InvariantCulture, out valor) ||
           double.TryParse(texto, NumberStyles.Any, new CultureInfo("pt-PT"), out valor);
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

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public HashSet<string> TiposSuportados { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public DateTime LastSync { get; set; }
}