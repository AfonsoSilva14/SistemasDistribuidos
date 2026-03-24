using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

Console.OutputEncoding = Encoding.UTF8;

const string gatewayId = "GW01";
const string serverIp = "127.0.0.1";
const int serverPort = 5000;
const int gatewayPort = 6000;

// Sensor simples "registado"
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

// Fica à escuta para sensores
TcpListener listener = new TcpListener(IPAddress.Any, gatewayPort);
listener.Start();

Console.WriteLine($"[GATEWAY] À escuta de sensores na porta {gatewayPort}...");

while (true)
{
    TcpClient sensorClient = listener.AcceptTcpClient();
    Console.WriteLine("[GATEWAY] Sensor ligado.");
    _ = Task.Run(() => HandleSensor(sensorClient, sensorDb, gatewayId, serverWriter, serverReader));
}

static void HandleSensor(
    TcpClient client,
    Dictionary<string, SensorInfo> sensorDb,
    string gatewayId,
    StreamWriter serverWriter,
    StreamReader serverReader)
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
                            string reason = sensor.Estado.Equals("manutenção", StringComparison.OrdinalIgnoreCase) ||
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
                            if (sensor.TiposSuportados.Contains(tipo))
                                registeredTypes.Add(tipo);
                            else
                                invalidos.Add(tipo);
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
                        string tipo = parts[3];
                        string valor = parts[4];
                        string unidade = parts[5];

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

                        SensorInfo sensor = sensorDb[currentSensorId];
                        sensor.LastSync = DateTime.Now;

                        string localLog = $"{timestamp}|{sensorId}|{sensor.Zona}|{tipo}|{valor}|{unidade}";
                        File.AppendAllText("dados_gateway.txt", localLog + Environment.NewLine);

                        writer.WriteLine($"DATA_ACK|{sensorId}|{timestamp}|OK");

                        string gwData = $"GW_DATA|{gatewayId}|{sensorId}|{sensor.Zona}|{timestamp}|{tipo}|{valor}|{unidade}";
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

class SensorInfo
{
    public string SensorId { get; set; } = "";
    public string Estado { get; set; } = "";
    public string Zona { get; set; } = "";
    public HashSet<string> TiposSuportados { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public DateTime LastSync { get; set; }
}