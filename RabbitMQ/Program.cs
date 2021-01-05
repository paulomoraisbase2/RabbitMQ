using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace RabbitMQ
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration().WriteTo.Console().CreateLogger();

            var conn = Connect.GetConnection();

            if (conn.IsOpen)
            {
                IModel channel = conn.CreateModel();

                channel.ExchangeDeclare("Rota", ExchangeType.Fanout);
                channel.QueueDeclare("Fila", false, false, false, null);
                //channel.QueueBind("Fila", "Rota", "/", null);
                FanoutReceive(channel, 1);
                FanoutReceive(channel, 2);
                Thread.Sleep(6000);
                FanoutSender(channel);

                _ = Console.ReadLine();
            }
        }

        public static void FanoutSender(IModel channel)
        {
            IBasicProperties props = channel.CreateBasicProperties();
            props.ContentType = "text/plain";
            props.DeliveryMode = 2;
            props.Headers = new Dictionary<string, object>();
            props.Headers.Add("latitude", 51.5252949);
            props.Headers.Add("longitude", -0.0905493);
            channel.ExchangeDeclare(exchange: "Rota", type: ExchangeType.Fanout);

            string[] messages = { "Primeira mensagem", "Segunda mensagem", "Terceira mensagem", "Quarta mensagem", "Quinta mensagem" };

            foreach (var item in messages)
            {
                var body = Encoding.UTF8.GetBytes(item);
                channel.BasicPublish(exchange: "Rota",
                                     routingKey: "Fila",
                                     basicProperties: null,
                                     body: body);
                Log.Information(" [+] Enviando {0}", item);
            }
        }

        public static void FanoutReceive(IModel channel, int fila)
        {
            channel.ExchangeDeclare(exchange: "Rota", type: ExchangeType.Fanout);

            var queueName = channel.QueueDeclarePassive("Fila").QueueName;

            Log.Warning($" [*] Fila {fila} Esperando por registros.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);
                channel.QueueBind(queue: queueName,
                              exchange: "Rota",
                              routingKey: "/");

                Log.Information($" [{fila}] {message}");
            };
            channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);
        }
    }
}