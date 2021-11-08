using DevFreela.Payments.API.Models;
using DevFreela.Payments.API.Services;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace DevFreela.Payments.API.Consumers
{
    public class ProcessPaymentConsumer : BackgroundService
    {
        private const string PAYMENTS_QUEUE = "Payments";
        private const string APPROVED_PAYMENTS_QUEUE = "ApprovedPayments";


        private readonly IConnection _connection;
        private readonly IModel _channel;
        private readonly IServiceProvider _serviceProvider;

        public ProcessPaymentConsumer(IServiceProvider serviceProvider)
        {
            _serviceProvider = serviceProvider;

            var _factory = new ConnectionFactory { HostName = "127.0.0.1" };

            _connection = _factory.CreateConnection();
            _channel = _connection.CreateModel();
            
            _channel.QueueDeclare(
                        queue: PAYMENTS_QUEUE,
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);

            _channel.QueueDeclare(
                        queue: APPROVED_PAYMENTS_QUEUE,
                        durable: false,
                        exclusive: false,
                        autoDelete: false,
                        arguments: null);
        }

        protected override Task ExecuteAsync(CancellationToken stoppingToken)
        {
            var consumer = new EventingBasicConsumer(_channel);

            consumer.Received += (sender, eventArgs) =>
            {
                var byteArray = eventArgs.Body.ToArray();
                var paymentInfoJson = Encoding.UTF8.GetString(byteArray);

                var paymentInfo = JsonSerializer.Deserialize<PaymentInfoInputModel>(paymentInfoJson);

                ProcessPayment(paymentInfo);

                var approvedPayment = new ApprovedPaymentIntegrationEvent(paymentInfo.IdProject);
                var approvedPaymentJson = JsonSerializer.Serialize(approvedPayment);
                var approvedPaymentBytes = Encoding.UTF8.GetBytes(approvedPaymentJson);

                _channel.BasicPublish(
                        exchange: "",
                        routingKey: APPROVED_PAYMENTS_QUEUE,
                        basicProperties: null,
                        body: approvedPaymentBytes);

                _channel.BasicAck(eventArgs.DeliveryTag, false);
            };

            _channel.BasicConsume(PAYMENTS_QUEUE, false, consumer);

            return Task.CompletedTask;
        }

        private void ProcessPayment(PaymentInfoInputModel paymentInfo)
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                var paymentService = scope.ServiceProvider.GetRequiredService<IPaymentService>();

                paymentService.Process(paymentInfo);
            }
        }
    }
}
