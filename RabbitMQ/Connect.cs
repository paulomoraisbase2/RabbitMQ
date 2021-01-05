using RabbitMQ.Client;

namespace RabbitMQ
{
    public class Connect
    {
        public static IConnection GetConnection()
        {
            ConnectionFactory factory = new ConnectionFactory
            {
                UserName = "guest",
                Password = "guest",
                HostName = "localHost",
                Port = 5672
            };
            //factory.DispatchConsumersAsync = true;

            return factory.CreateConnection();
        }
    }
}