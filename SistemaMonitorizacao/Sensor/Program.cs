using System;
using System.IO;
using System.Net.Sockets;
using System.Text;

Console.OutputEncoding = Encoding.UTF8;

string gatewayIp = "127.0.0.1";
int gatewayPort = 6000;

string sensorId = "S102";
string zona = "ZONA_ESCOLAR";

TcpClient client = new TcpClient();
client.Connect(gatewayIp, gatewayPort);

using NetworkStream ns = client.GetStream();
using StreamReader reader = new StreamReader(ns, Encoding.UTF8);
using StreamWriter writer = new StreamWriter(ns, Encoding.UTF8) { AutoFlush = true };

Console.WriteLine("[SENSOR] Ligado ao gateway.");
Console.WriteLine();

bool running = true;

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
                SendAndRead(msg, writer, reader, expectedLines: 2);
                break;
            }

        case "2":
            {
                Console.Write("Tipos de dados (ex: TEMP,PM25): ");
                string? tipos = Console.ReadLine();

                string msg = $"REGISTER_TYPES|{sensorId}|{tipos}";
                SendAndRead(msg, writer, reader, expectedLines: 1);
                break;
            }

        case "3":
            {
                Console.Write("Tipo de dado: ");
                string? tipo = Console.ReadLine();

                Console.Write("Valor: ");
                string? valor = Console.ReadLine();

                Console.Write("Unidade: ");
                string? unidade = Console.ReadLine();

                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string msg = $"DATA|{sensorId}|{timestamp}|{tipo}|{valor}|{unidade}";
                SendAndRead(msg, writer, reader, expectedLines: 1);
                break;
            }

        case "4":
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string msg = $"HEARTBEAT|{sensorId}|{timestamp}";
                SendAndRead(msg, writer, reader, expectedLines: 1);
                break;
            }

        case "5":
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
                string msg = $"BYE|{sensorId}|{timestamp}";
                SendAndRead(msg, writer, reader, expectedLines: 1);
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

static void SendAndRead(string message, StreamWriter writer, StreamReader reader, int expectedLines)
{
    Console.WriteLine("[SENSOR] Enviado: " + message);
    writer.WriteLine(message);

    for (int i = 0; i < expectedLines; i++)
    {
        string? response = reader.ReadLine();
        if (response == null)
        {
            Console.WriteLine("[SENSOR] Ligação fechada.");
            return;
        }

        Console.WriteLine("[SENSOR] Resposta: " + response);
    }
}