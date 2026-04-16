using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

Console.OutputEncoding = Encoding.UTF8;

// Parâmetros de inicialização: [gatewayIp] [sensorId] [zona]
// Exemplo: dotnet run -- 127.0.0.1 S102 ZONA_ESCOLAR
string gatewayIp = args.Length > 0 ? args[0] : "127.0.0.1";
int gatewayPort = 6000;
string sensorId = args.Length > 1 ? args[1] : "S102";
string zona = args.Length > 2 ? args[2] : "ZONA_ESCOLAR";

TcpClient client = new TcpClient();
client.Connect(gatewayIp, gatewayPort);

using NetworkStream ns = client.GetStream();
using StreamReader reader = new StreamReader(ns, Encoding.UTF8);
using StreamWriter writer = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };

Console.WriteLine($"[SENSOR] Ligado ao gateway {gatewayIp}:{gatewayPort}.");
Console.WriteLine($"[SENSOR] ID: {sensorId} | Zona: {zona}");
Console.WriteLine();

// Lock para acesso thread-safe ao stream de rede (menu + heartbeat automático)
object networkLock = new object();
bool running = true;
bool authenticated = false;

// Heartbeat automático a cada 30 segundos (só depois de autenticado)
_ = Task.Run(async () =>
{
    while (running)
    {
        await Task.Delay(30000);
        if (running && authenticated)
        {
            string ts = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            string msg = $"HEARTBEAT|{sensorId}|{ts}";
            Console.WriteLine();
            SendAndRead(msg, writer, reader, 1, networkLock);
            Console.WriteLine("[SENSOR] (heartbeat automático)");
        }
    }
});

while (running)
{
    Console.WriteLine("=== MENU SENSOR ===");
    Console.WriteLine("1 - Iniciar comunicação (HELLO)");
    Console.WriteLine("2 - Registar tipos (REGISTER_TYPES)");
    Console.WriteLine("3 - Enviar dado (DATA)");
    Console.WriteLine("4 - Enviar heartbeat");
    Console.WriteLine("5 - Terminar comunicação (BYE)");
    Console.WriteLine("0 - Sair");
    Console.Write("Opção: ");

    string? option = Console.ReadLine();
    Console.WriteLine();

    switch (option)
    {
        case "1":
            {
                string msg = $"HELLO|{sensorId}|{zona}";
                var responses = SendAndRead(msg, writer, reader, 2, networkLock);
                if (responses.Exists(r => r.StartsWith("AUTH_OK")))
                {
                    authenticated = true;
                    Console.WriteLine("[SENSOR] Autenticado com sucesso.");
                }
                break;
            }

        case "2":
            {
                Console.Write("Tipos de dados (ex: TEMP,PM25,RUIDO): ");
                string? tipos = Console.ReadLine();
                string msg = $"REGISTER_TYPES|{sensorId}|{tipos}";
                SendAndRead(msg, writer, reader, 1, networkLock);
                break;
            }

        case "3":
            {
                Console.Write("Tipo de dado (TEMP/HUM/CO2/PRESS/PM25/PM10/RUIDO/AR/LUZ): ");
                string? tipo = Console.ReadLine();

                Console.Write("Valor: ");
                string? valor = Console.ReadLine();

                Console.Write("Unidade: ");
                string? unidade = Console.ReadLine();

                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string msg = $"DATA|{sensorId}|{timestamp}|{tipo}|{valor}|{unidade}";
                SendAndRead(msg, writer, reader, 1, networkLock);
                break;
            }

        case "4":
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string msg = $"HEARTBEAT|{sensorId}|{timestamp}";
                SendAndRead(msg, writer, reader, 1, networkLock);
                break;
            }

        case "5":
            {
                authenticated = false;
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string msg = $"BYE|{sensorId}|{timestamp}";
                SendAndRead(msg, writer, reader, 1, networkLock);
                running = false;
                break;
            }

        case "0":
            running = false;
            break;

        default:
            Console.WriteLine("Opção inválida.");
            break;
    }

    Console.WriteLine();
}

client.Close();
Console.WriteLine("[SENSOR] Programa terminado.");

static List<string> SendAndRead(string message, StreamWriter writer, StreamReader reader, int expectedLines, object networkLock)
{
    var responses = new List<string>();
    lock (networkLock)
    {
        Console.WriteLine("[SENSOR] Enviado: " + message);
        writer.WriteLine(message);

        for (int i = 0; i < expectedLines; i++)
        {
            string? response = reader.ReadLine();
            if (response == null)
            {
                Console.WriteLine("[SENSOR] Ligação fechada.");
                return responses;
            }

            Console.WriteLine("[SENSOR] Resposta: " + response);
            responses.Add(response);
        }
    }
    return responses;
}
