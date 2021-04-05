using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Confluent.Kafka;

namespace MessageBroker.Kafka.Lib
{
    public sealed class MessageBus : IDisposable
    {
        private readonly IProducer<Null, string> _producer;
        private IConsumer<Null, string> _consumer;

        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public MessageBus() : this("localhost") { }

        public MessageBus(string host)
        {
            _producerConfig = new ProducerConfig()
            {
                BootstrapServers = host
            };

            _consumerConfig =  new ConsumerConfig
            {
                BootstrapServers = host,
                GroupId = "custom-group",
            }; 
             
            _producer = new ProducerBuilder<Null, string>(_producerConfig).Build();
        }

        public void SendMessage(string topic, string message)
        {
            _producer.ProduceAsync(topic, new Message<Null, string> { Value = message } );            
        }

        public void SubscribeOnTopic(string topic, Action<string> action,Action<Exception> errAction , CancellationToken cancellationToken) 
        {
            var msgBus = new MessageBus();
            using (msgBus._consumer = new ConsumerBuilder<Null, string>(_consumerConfig).Build())
            {
                msgBus._consumer.Assign(new List<TopicPartitionOffset> { new TopicPartitionOffset(topic, 0, -1) });

                while (true)
                {
                    if (cancellationToken.IsCancellationRequested)
                        break;

                    try
                    {
                        var consumeResult = msgBus._consumer.Consume(cancellationToken);
                        
                        action(consumeResult.Message.Value);
                    }
                    catch(Exception ex)
                    {
                        if (errAction != null)
                        {
                            errAction(ex);
                        }
                    }                 

                    
                    /*Message<Null, string> msg;
                    if (msgBus._consumer.Consume(TimeSpan.FromMilliseconds(10)))
                    {
                        action(msg.Value as T);
                    }*/
                }
                msgBus._consumer.Close();
            }
        }

        public void Dispose()
        {
            _producer?.Dispose();
            _consumer?.Dispose();
        }
    }
}
