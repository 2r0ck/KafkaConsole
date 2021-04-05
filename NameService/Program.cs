using MessageBroker.Kafka.Lib;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace NameService
{
    class Program
    {
        private static MessageBus msgBus;
        private static readonly string userHelpMsg = "NameService.\nEnter 'b' or 'g' to process boy or girl names respectively";
        private static readonly string bTopicNameCmd = "b_name_command";
        private static readonly string gTopicNameCmd = "g_name_command";

        private static readonly string bTopicNameResp = "b_name_response";
        private static readonly string gTopicNameResp = "g_name_response";
        private static readonly string[] _boyNames =
        {
            "Arsenii",
            "Igor",
            "Kostya",
            "Ivan",
            "Dmitrii",
        };
        private static readonly string[] _girlNames =
        {
            "Nastya",
            "Lena",
            "Ksusha",
            "Katya",
            "Olga"
        };

        static void Main(string[] args)
        {
            bool canceled = false;

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true;
                canceled = true;
            };

            using (msgBus = new MessageBus())
            {
                Console.WriteLine(userHelpMsg);

                HandleUserInput(Console.ReadLine());

                while (!canceled) { }
            }
        }

        private static void HandleUserInput(string userInput)
        {
            switch (userInput)
            {
                case "b":
                    Task.Run(() => msgBus.SubscribeOnTopic(bTopicNameCmd, (msg) => BoyNameCommandListener(msg), (ex) => { Console.WriteLine(ex); }, CancellationToken.None));
                    Console.WriteLine("Processing boy names");
                    break;
                case "g":
                    Task gTask = Task.Run(() => msgBus.SubscribeOnTopic(gTopicNameCmd, (msg) => GirlNameCommandListener(msg),(ex) => { Console.WriteLine(ex); }, CancellationToken.None));
                    Console.WriteLine("Processing girl names");
                    break;
                default:
                    Console.WriteLine($"Unknown command. {userHelpMsg}");
                    HandleUserInput(Console.ReadLine());
                    break;
            }
        }

        private static void BoyNameCommandListener(string msg)
        {
            var r = new Random().Next(0, 5);
            var randName = _boyNames[r];

            msgBus.SendMessage(bTopicNameResp, randName);
            Console.WriteLine($"Sending {randName}");
        }

        private static void GirlNameCommandListener(string msg)
        {
            var r = new Random().Next(0, 5);
            var randName = _girlNames[r];

            msgBus.SendMessage(gTopicNameResp, randName);
            Console.WriteLine($"Sending {randName}");
        }
    }
}
