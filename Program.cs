using System;
using System.Diagnostics;
using System.Text;
using System.Threading.Tasks;
using Azure.Messaging.EventHubs;
using Azure.Messaging.EventHubs.Consumer;
using Azure.Messaging.EventHubs.Producer;
using Microsoft.Extensions.Configuration;

public class Program
{

    static IConfiguration config = new ConfigurationBuilder()
                .AddJsonFile("appsettings.json")
                .AddJsonFile("appsettings.local.json", false)
                .AddEnvironmentVariables()
                .Build();
    public static void Main(string[] args)
    {
        var obj = new Program();

        obj.ReadEvents().GetAwaiter().GetResult();
        Console.WriteLine("Press enter to continue...");
        Console.ReadLine();
    }

    async Task ReadEvents()
    {
        // Settings settings = config.GetRequiredSection("Settings").Get<Settings>();

        var settings = config.AsEnumerable().AsQueryable();

        var connectionString = settings.Where( f => f.Key == "Settings:connectinoString").First().Value;
        var eventHubName = settings.Where( f => f.Key == "Settings:eventHubName").First().Value;
        var consumerGroup = settings.Where( f => f.Key == "Settings:consumerGroup").First().Value;

        var consumer = new EventHubConsumerClient(
            consumerGroup,
            connectionString,
            eventHubName);

        try
        {
            using CancellationTokenSource cancellationSource = new CancellationTokenSource();
            cancellationSource.CancelAfter(TimeSpan.FromSeconds(30));

            string firstPartition = "1"; //(await consumer.GetPartitionIdsAsync(cancellationSource.Token)).First();
            int partitionCount = (await consumer.GetPartitionIdsAsync(cancellationSource.Token)).Count();
            EventPosition startingPosition = EventPosition.Earliest;

            await foreach (PartitionEvent partitionEvent in consumer.ReadEventsFromPartitionAsync(
                firstPartition,
                startingPosition,
                cancellationSource.Token))
            {
                string readFromPartition = partitionEvent.Partition.PartitionId;
                //ReadOnlyMemory<byte> eventBodyBytes = partitionEvent.Data.EventBody.ToMemory();
                string partitionKey = partitionEvent.Data.PartitionKey; //.EventBody.ToArray();
                int calculatedParitionId = HashCode(partitionKey, partitionCount);

                if (int.Parse(readFromPartition) != calculatedParitionId)
                    Console.WriteLine($"actual partitino {readFromPartition} for {partitionKey} calculated {calculatedParitionId}");
                
            }
        }
        catch (TaskCanceledException)
        {
            // This is expected if the cancellation token is
            // signaled.
        }
        finally
        {
            await consumer.CloseAsync();
        }

    }

    int HashCode(String partitionKey, int partitionCount){
        return Math.Abs(partitionKey.GetHashCode() % partitionCount);
    }
}