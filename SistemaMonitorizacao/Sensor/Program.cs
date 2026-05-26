using System;
using System.Text;
using System.Threading.Tasks;
using RabbitMQ.Client;

Console.OutputEncoding = Encoding.UTF8;

// Parâmetros: [sensorId] [zona]
// Exemplo: dotnet run -- S102 ZONA_ESCOLAR
string sensorId = args.Length > 0 ? args[0] : "S102";
string zona = args.Length > 1 ? args[1] : "ZONA_ESCOLAR";
bool autoMode = args.Any(a => a.Equals("--auto", StringComparison.OrdinalIgnoreCase));
int autoIntervalSeconds = LerInteiroOpcional(args, "--interval", 5);

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

if (autoMode)
{
    await ExecutarModoAutomatico(
        channel,
        exchangeName,
        sensorId,
        zona,
        autoIntervalSeconds,
        publishLock);

    running = false;
}

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

static async Task ExecutarModoAutomatico(
    IModel channel,
    string exchangeName,
    string sensorId,
    string zona,
    int intervaloSegundos,
    object publishLock)
{
    var random = new Random();
    var tipos = new[]
    {
        ("TEMP", "C", 18.0, 38.0),
        ("HUM", "%", 35.0, 95.0),
        ("CO2", "ppm", 400.0, 2200.0),
        ("PM25", "ug/m3", 5.0, 45.0),
        ("PM10", "ug/m3", 10.0, 90.0),
        ("PRESS", "hPa", 980.0, 1035.0),
        ("RUIDO", "dB", 35.0, 90.0),
        ("LUZ", "lux", 80.0, 1200.0)
    };

    Console.WriteLine($"[SENSOR] Modo automatico ativo. Intervalo: {intervaloSegundos}s.");
    Console.WriteLine("[SENSOR] Pressiona CTRL+C para terminar.");

    while (true)
    {
        var tipo = tipos[random.Next(tipos.Length)];
        double valor = tipo.Item3 + random.NextDouble() * (tipo.Item4 - tipo.Item3);
        string timestamp = DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss");
        string msg = $"DATA|{sensorId}|{zona}|{timestamp}|{tipo.Item1}|{valor:F2}|{tipo.Item2}";
        string routingKey = $"{zona}.{tipo.Item1}".ToLowerInvariant();

        Publicar(channel, exchangeName, routingKey, msg, publishLock);

        string heartbeat = $"HEARTBEAT|{sensorId}|{zona}|{timestamp}";
        Publicar(channel, exchangeName, $"{zona}.heartbeat".ToLowerInvariant(), heartbeat, publishLock);

        await Task.Delay(TimeSpan.FromSeconds(Math.Max(1, intervaloSegundos)));
    }
}

static int LerInteiroOpcional(string[] args, string nome, int valorPorDefeito)
{
    for (int i = 0; i < args.Length - 1; i++)
    {
        if (args[i].Equals(nome, StringComparison.OrdinalIgnoreCase) &&
            int.TryParse(args[i + 1], out int valor) &&
            valor > 0)
        {
            return valor;
        }
    }

    return valorPorDefeito;
}

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
