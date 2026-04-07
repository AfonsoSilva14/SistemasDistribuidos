using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text;

Console.OutputEncoding = Encoding.UTF8;

const int serverPort = 5000;

TcpListener listener = new TcpListener(IPAddress.Loopback, serverPort);
listener.Start();

Console.WriteLine($"[SERVIDOR] À escuta na porta {serverPort}...");

while (true)
{
    TcpClient gatewayClient = listener.AcceptTcpClient();
    Console.WriteLine("[SERVIDOR] Gateway ligado.");

    _ = Task.Run(() => HandleGateway(gatewayClient));
}

static void HandleGateway(TcpClient client)
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
                            File.AppendAllText("dados_servidor.txt", logLine + Environment.NewLine);

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