using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

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
                var channel = conn.CreateModel();
                FanoutReceive(channel, 1);
                FanoutReceive(channel, 2);
                FanoutSender(channel);

                Thread.Sleep(6000);

                var channelDirect = conn.CreateModel();
                DirectReceive(channelDirect, new string[] { "error" });
                DirectReceive(channelDirect, new string[] { "warning", "info" });
                DirectSender(channelDirect);

                Thread.Sleep(6000);

                var channelTopic = conn.CreateModel();
                TopicReceive(channelTopic, "A.*");
                TopicReceive(channelTopic, "*.error");
                TopicReceive(channelTopic, "#");
                TopicSender(channelTopic);

                _ = Console.ReadLine();
            }
        }

        public static void FanoutSender(IModel channel)
        {
            IBasicProperties props = channel.CreateBasicProperties();
            props.ContentType = "text/plain";
            props.DeliveryMode = 2;
            channel.ExchangeDeclare(exchange: "Rota", type: ExchangeType.Fanout);

            string[] messages = { "Primeira mensagem", "Segunda mensagem", "Terceira mensagem", "Quarta mensagem", "Quinta mensagem" };

            foreach (var item in messages)
            {
                props.Headers = new Dictionary<string, object>();
                props.Headers.Add("Guid", Guid.NewGuid().ToString());
                var body = Encoding.UTF8.GetBytes(item);
                channel.BasicPublish(exchange: "Rota",
                                     routingKey: "",
                                     basicProperties: props,
                                     body: body);
                Log.Information(" [+] Enviando {0}", item);
            }
        }

        public static void FanoutReceive(IModel channel, int fila)
        {
            channel.ExchangeDeclare(exchange: "Rota", type: ExchangeType.Fanout);

            var queueName = channel.QueueDeclare("").QueueName;

            Log.Warning($" [*] Fila {fila} Esperando por registros.");

            channel.QueueBind(queue: queueName,
                               exchange: "Rota",
                               routingKey: "");
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var head = ea.BasicProperties.Headers;
                var message = Encoding.UTF8.GetString(body);

                Log.Information($" [{fila}] {message}:{(head != null ? Encoding.UTF8.GetString((byte[])head["Guid"]) : "")}");
            };
            channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);
        }

        public static void DirectSender(IModel channel)
        {
            IBasicProperties props = channel.CreateBasicProperties();
            props.ContentType = "text/plain";
            props.DeliveryMode = 2;
            channel.ExchangeDeclare(exchange: "Rota_Direta", type: ExchangeType.Direct);

            string[] messages = { "Primeira mensagem", "Segunda mensagem", "Terceira mensagem", "Quarta mensagem", "Quinta mensagem" };
            string[] severities = { "info", "error", "warning", "error", "info" };

            foreach (var item in messages.Select((message, index) => new { message, index }))
            {
                props.Headers = new Dictionary<string, object>();
                props.Headers.Add("Guid", Guid.NewGuid().ToString());
                var body = Encoding.UTF8.GetBytes(item.message);
                channel.BasicPublish(exchange: "Rota_Direta",
                                     routingKey: severities[item.index],
                                     basicProperties: props,
                                     body: body);
                Log.Information($" [+] Enviando {item.message}:{severities[item.index]}");
            }
        }

        public static void DirectReceive(IModel channel, string[] severities)
        {
            channel.ExchangeDeclare(exchange: "Rota_Direta", type: ExchangeType.Direct);

            var queueName = channel.QueueDeclare("", true).QueueName;

            Log.Warning($" [*] Fila {severities.Length} Esperando por registros.");
            foreach (var item in severities)
            {
                channel.QueueBind(queue: queueName,
                                 exchange: "Rota_Direta",
                                 routingKey: item);
            }
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var head = ea.BasicProperties.Headers;
                var message = Encoding.UTF8.GetString(body);
                Log.Information($"[{severities.Length}]{ea.RoutingKey}:{message}");
            };
            channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);
        }

        public static void TopicSender(IModel channel)
        {
            IBasicProperties props = channel.CreateBasicProperties();
            props.ContentType = "text/plain";
            props.DeliveryMode = 2;
            channel.ExchangeDeclare(exchange: "Rota_Topic", type: ExchangeType.Topic);

            string[] messages = { "Primeira mensagem", "Segunda mensagem", "Terceira mensagem", "Quarta mensagem", "Quinta mensagem" };
            string[] severities = { "info", "error", "warning", "error", "info" };
            string[] components = { "A", "B", "A", "A", "B" };

            foreach (var item in messages.Select((message, index) => new { message, index }))
            {
                props.Headers = new Dictionary<string, object>();
                props.Headers.Add("Guid", Guid.NewGuid().ToString());
                var body = Encoding.UTF8.GetBytes(item.message);

                var routing_key = components[item.index] + "." + severities[item.index];
                channel.BasicPublish(exchange: "Rota_Topic",
                                     routingKey: routing_key,
                                     basicProperties: props,
                                     body: body);
                Log.Information($" [+] Enviando {item.message}:{severities[item.index]}");
            }
        }

        public static void TopicReceive(IModel channel, string componente)
        {
            channel.ExchangeDeclare(exchange: "Rota_Topic", type: ExchangeType.Topic);

            var queueName = channel.QueueDeclare("", true).QueueName;

            Log.Warning($" [*] Fila {componente} Esperando por registros.");

            channel.QueueBind(queue: queueName,
                                 exchange: "Rota_Topic",
                                 routingKey: componente);

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var head = ea.BasicProperties.Headers;
                var message = Encoding.UTF8.GetString(body);
                Log.Information($"[{componente}]{ea.RoutingKey}:{message}");
            };
            channel.BasicConsume(queue: queueName,
                                     autoAck: true,
                                     consumer: consumer);
        }
    }
}