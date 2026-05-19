using System;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

Console.OutputEncoding = Encoding.UTF8;

// Parâmetros: [sensorId] [zona]
// Exemplo: dotnet run -- S102 ZONA_ESCOLAR
string sensorId = args.Length > 0 ? args[0] : "S102";
string zona = args.Length > 1 ? args[1] : "ZONA_ESCOLAR";

const string exchangeName = "sensores_exchange";

var factory = new ConnectionFactory()
{
    HostName = "localhost"
};

using IConnection connection = factory.CreateConnection();
using IModel channel = connection.CreateModel();

channel.ExchangeDeclare(
    exchange: exchangeName,
    type: ExchangeType.Topic,
    durable: true,
    autoDelete: false
);

Console.WriteLine("[SENSOR] Ligado ao RabbitMQ.");
Console.WriteLine($"[SENSOR] ID: {sensorId} | Zona: {zona}");
Console.WriteLine();

object publishLock = new object();
bool running = true;

// Heartbeat automático a cada 30 segundos
_ = Task.Run(async () =>
{
    while (running)
    {
        await Task.Delay(30000);

        if (running)
        {
            string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
            string msg = $"HEARTBEAT|{sensorId}|{zona}|{timestamp}";

            Publicar(
                channel,
                exchangeName,
                $"{zona}.heartbeat".ToLowerInvariant(),
                msg,
                publishLock);

            Console.WriteLine("[SENSOR] Heartbeat automático enviado.");
        }
    }
});

while (running)
{
    Console.WriteLine("=== MENU SENSOR ===");
    Console.WriteLine("1 - Enviar dado (DATA)");
    Console.WriteLine("2 - Enviar heartbeat");
    Console.WriteLine("3 - Terminar sensor (BYE)");
    Console.WriteLine("0 - Sair");
    Console.Write("Opção: ");

    string? option = Console.ReadLine();
    Console.WriteLine();

    switch (option)
    {
        case "1":
            {
                Console.Write("Tipo de dado (TEMP/HUM/CO2/PRESS/PM25/PM10/RUIDO/AR/LUZ): ");
                string? tipo = Console.ReadLine();

                Console.Write("Valor: ");
                string? valor = Console.ReadLine();

                Console.Write("Unidade: ");
                string? unidade = Console.ReadLine();

                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

                string msg =
                    $"DATA|{sensorId}|{zona}|{timestamp}|{tipo}|{valor}|{unidade}";

                string routingKey =
                    $"{zona}.{tipo}".ToLowerInvariant();

                Publicar(
                    channel,
                    exchangeName,
                    routingKey,
                    msg,
                    publishLock);

                break;
            }

        case "2":
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

                string msg =
                    $"HEARTBEAT|{sensorId}|{zona}|{timestamp}";

                string routingKey =
                    $"{zona}.heartbeat".ToLowerInvariant();

                Publicar(
                    channel,
                    exchangeName,
                    routingKey,
                    msg,
                    publishLock);

                break;
            }

        case "3":
            {
                string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");

                string msg =
                    $"BYE|{sensorId}|{zona}|{timestamp}";

                string routingKey =
                    $"{zona}.control".ToLowerInvariant();

                Publicar(
                    channel,
                    exchangeName,
                    routingKey,
                    msg,
                    publishLock);

                running = false;
                break;
            }

        case "0":
            {
                running = false;
                break;
            }

        default:
            {
                Console.WriteLine("Opção inválida.");
                break;
            }
    }

    Console.WriteLine();
}

Console.WriteLine("[SENSOR] Programa terminado.");

static void Publicar(
    IModel channel,
    string exchangeName,
    string routingKey,
    string mensagem,
    object publishLock)
{
    byte[] body = Encoding.UTF8.GetBytes(mensagem);

    lock (publishLock)
    {
        channel.BasicPublish(
            exchange: exchangeName,
            routingKey: routingKey,
            basicProperties: null,
            body: body
        );
    }

    Console.WriteLine($"[SENSOR] Publicado em '{routingKey}': {mensagem}");
}