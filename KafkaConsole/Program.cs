using MessageBroker.Kafka.Lib;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaConsole
{
    class Program
    {
        private static readonly string bTopicNameCmd = "b_name_command";
        private static readonly string gTopicNameCmd = "g_name_command";
        private static readonly string bMessageReq = "get_boy_name";
        private static readonly string gMessageReq = "get_girl_name";

        private static readonly string bTopicNameResp = "b_name_response";
        private static readonly string gTopicNameResp = "g_name_response";

        private static readonly string userHelpMsg = "MainApp: Enter 'b' for a boy or 'g' for a girl, 'q' to exit";

        static void Main(string[] args)
        {
            using (var msgBus = new MessageBus())
            {
                Task.Run(() => msgBus.SubscribeOnTopic(bTopicNameResp, msg => GetBoyNameHandler(msg), (ex) => { Console.WriteLine(ex); }, CancellationToken.None));
                Task.Run(() => msgBus.SubscribeOnTopic(gTopicNameResp, msg => GetGirlNameHandler(msg), (ex) => { Console.WriteLine(ex); }, CancellationToken.None));

                string userInput;

                do
                {
                    Console.WriteLine(userHelpMsg);
                    userInput = Console.ReadLine();
                    switch (userInput)
                    {
                        case "b":
                            msgBus.SendMessage(topic: bTopicNameCmd, message: bMessageReq);
                            break;
                        case "g":
                            msgBus.SendMessage(topic: gTopicNameCmd, message: gMessageReq);
                            break;
                        case "q":
                            break;
                        default:
                            Console.WriteLine($"Unknown command. {userHelpMsg}");
                            break;
                    }

                } while (userInput != "q");
            }
        }

        public static void GetBoyNameHandler(string msg)
        {
            Console.WriteLine($"Boy name {msg} is recommended");
        }

        public static void GetGirlNameHandler(string msg)
        {
            Console.WriteLine($"Girl name {msg} is recommended");
        }
    }
}
