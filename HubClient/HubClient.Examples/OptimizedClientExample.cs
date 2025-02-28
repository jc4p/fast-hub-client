using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using HubClient.Core.Models;
using HubClient.Production;
using HubClient.Production.Concurrency;
using HubClient.Production.Grpc;
using Microsoft.Extensions.Logging;

namespace HubClient.Examples
{
    /// <summary>
    /// Example demonstrating the usage of the OptimizedHubClient
    /// </summary>
    public class OptimizedClientExample
    {
        private readonly ILogger<OptimizedClientExample> _logger;

        public OptimizedClientExample(ILogger<OptimizedClientExample> logger = null)
        {
            _logger = logger;
        }

        /// <summary>
        /// Run the example with default settings
        /// </summary>
        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            // Default server endpoint for the example
            string serverEndpoint = "http://localhost:5000";

            // Create client options with optimal settings from benchmarks
            var options = new OptimizedHubClientOptions
            {
                ServerEndpoint = serverEndpoint,
                ChannelCount = 8,                  // Optimal from benchmarks
                MaxConcurrentCallsPerChannel = 1000,
                PipelineStrategy = PipelineStrategy.Channel,  // Most balanced approach
                PipelineInputQueueCapacity = 25000,  // Good for large workloads
                PipelineOutputQueueCapacity = 25000,
                MaxPipelineConcurrency = Math.Min(Environment.ProcessorCount, 16)
            };

            await RunWithOptionsAsync(options, cancellationToken);
        }

        /// <summary>
        /// Run the example with custom options
        /// </summary>
        public async Task RunWithOptionsAsync(OptimizedHubClientOptions options, CancellationToken cancellationToken = default)
        {
            _logger?.LogInformation("Starting OptimizedHubClient example with endpoint: {Endpoint}", options.ServerEndpoint);

            // Create the optimized client
            using var client = new OptimizedHubClient(options, _logger);

            try
            {
                // Initialize the client
                await client.InitializeAsync(cancellationToken);
                _logger?.LogInformation("Client initialized successfully");

                // Example 1: Send a direct request
                await SendDirectRequestExample(client, cancellationToken);

                // Example 2: Process a stream through a pipeline
                await ProcessStreamExample(client, cancellationToken);

                // Example 3: Use a strongly-typed client
                await UseStronglyTypedClientExample(client, cancellationToken);

                _logger?.LogInformation("OptimizedHubClient examples completed successfully");
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in OptimizedHubClient example");
                throw;
            }
        }

        private async Task SendDirectRequestExample(OptimizedHubClient client, CancellationToken cancellationToken)
        {
            _logger?.LogInformation("Running direct request example...");

            // Create a request message (this is just an example, actual message would depend on your proto)
            var request = new ExampleRequest
            {
                Id = 12345,
                Name = "Test Request"
            };

            // Define how to execute the call
            var callFunc = async (ExampleRequest req, CancellationToken ct) =>
            {
                // In a real implementation, this would use the gRPC client to make the call
                // For this example, we'll just create a mock response
                await Task.Delay(100, ct); // Simulate network delay
                
                return new ExampleResponse
                {
                    Success = true,
                    Message = $"Processed request for {req.Name} with ID {req.Id}",
                    Timestamp = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(
                        DateTime.UtcNow.ToUniversalTime())
                };
            };

            // Send the request using the client
            var response = await client.SendRequestAsync(
                request,
                "ExampleDirectRequest", // Operation key
                callFunc,
                TimeSpan.FromSeconds(10), // Timeout
                cancellationToken);

            _logger?.LogInformation("Received response: {Message}", response.Message);
        }

        private async Task ProcessStreamExample(OptimizedHubClient client, CancellationToken cancellationToken)
        {
            _logger?.LogInformation("Running stream processing example...");

            // Create a request message
            var request = new ExampleStreamRequest
            {
                Count = 100, // Request 100 messages
                Interval = 10 // 10ms between messages
            };

            // Define a list to collect processed messages
            var processedMessages = new List<string>();

            // Define how to execute the streaming call
            var streamingCallFunc = (ExampleStreamRequest req, CancellationToken ct) =>
            {
                // This would normally use a real gRPC client with streaming
                // For this example, we'll create a mock streaming call

                // Create a stream of responses
                var responseStream = new MockAsyncStreamingCall<ExampleStreamResponse>(req.Count, req.Interval);
                
                return responseStream;
            };

            // Define how to process each message
            var messageProcessor = new Func<ExampleStreamResponse, ValueTask>(response =>
            {
                processedMessages.Add(response.Message);
                _logger?.LogDebug("Processed message: {Message}", response.Message);
                return new ValueTask();
            });

            // Process the stream using the client
            int processedCount = await client.ProcessStreamAsync(
                request,
                "ExampleStreamProcessing", // Operation key
                streamingCallFunc,
                messageProcessor,
                TimeSpan.FromMinutes(1), // Timeout
                cancellationToken);

            _logger?.LogInformation("Processed {Count} messages from stream", processedCount);
            
            // Get metrics from the pipeline
            var metrics = client.GetPipelineMetrics("ExampleStreamProcessing");
            if (metrics != null)
            {
                _logger?.LogInformation(
                    "Pipeline metrics - Processed: {Processed}, Enqueued: {Enqueued}, Processing Latency: {LatencyMs}ms",
                    metrics.TotalItemsProcessed,
                    metrics.TotalItemsEnqueued,
                    metrics.AverageProcessingLatencyMs);
            }
        }

        private async Task UseStronglyTypedClientExample(OptimizedHubClient client, CancellationToken cancellationToken)
        {
            _logger?.LogInformation("Running strongly-typed client example...");

            // Get a strongly-typed client from the OptimizedHubClient
            // This would normally be your generated gRPC client class
            // For this example, we'll use a mock client
            var resilientClient = client.CreateClient<MockGrpcClient>();

            // Use the resilient client to make calls
            try
            {
                var response = await resilientClient.ExecuteAsync<string>(
                    "GetData", // Operation key
                    mockClient => Task.FromResult("Mock data from gRPC service"),
                    TimeSpan.FromSeconds(5), // Timeout
                    cancellationToken);

                _logger?.LogInformation("Received response from strongly-typed client: {Response}", response);
            }
            catch (Exception ex)
            {
                _logger?.LogError(ex, "Error in strongly-typed client example");
                throw;
            }
        }

        #region Mock Classes for Example

        // Mock classes to demonstrate the example without requiring actual proto-generated classes

        public class ExampleRequest : IMessage<ExampleRequest>
        {
            public int Id { get; set; }
            public string Name { get; set; }

            public ExampleRequest Clone() => new ExampleRequest { Id = Id, Name = Name };
            public void MergeFrom(ExampleRequest message) { Id = message.Id; Name = message.Name; }
            public int CalculateSize() => 0;
            public void WriteTo(CodedOutputStream output) { }
            public MessageDescriptor Descriptor => null;
            public int Id { get; set; }
            public string Name { get; set; }
        }

        public class ExampleResponse : IMessage<ExampleResponse>
        {
            public bool Success { get; set; }
            public string Message { get; set; }
            public Google.Protobuf.WellKnownTypes.Timestamp Timestamp { get; set; }

            public ExampleResponse Clone() => new ExampleResponse { Success = Success, Message = Message, Timestamp = Timestamp?.Clone() };
            public void MergeFrom(ExampleResponse message) { Success = message.Success; Message = message.Message; Timestamp = message.Timestamp?.Clone(); }
            public int CalculateSize() => 0;
            public void WriteTo(CodedOutputStream output) { }
            public MessageDescriptor Descriptor => null;
        }

        public class ExampleStreamRequest : IMessage<ExampleStreamRequest>
        {
            public int Count { get; set; }
            public int Interval { get; set; }

            public ExampleStreamRequest Clone() => new ExampleStreamRequest { Count = Count, Interval = Interval };
            public void MergeFrom(ExampleStreamRequest message) { Count = message.Count; Interval = message.Interval; }
            public int CalculateSize() => 0;
            public void WriteTo(CodedOutputStream output) { }
            public MessageDescriptor Descriptor => null;
        }

        public class ExampleStreamResponse : IMessage<ExampleStreamResponse>
        {
            public int SequenceNumber { get; set; }
            public string Message { get; set; }
            public Google.Protobuf.WellKnownTypes.Timestamp Timestamp { get; set; }

            public ExampleStreamResponse Clone() => new ExampleStreamResponse 
            { 
                SequenceNumber = SequenceNumber, 
                Message = Message, 
                Timestamp = Timestamp?.Clone() 
            };
            public void MergeFrom(ExampleStreamResponse message) 
            { 
                SequenceNumber = message.SequenceNumber; 
                Message = message.Message; 
                Timestamp = message.Timestamp?.Clone(); 
            }
            public int CalculateSize() => 0;
            public void WriteTo(CodedOutputStream output) { }
            public MessageDescriptor Descriptor => null;
        }

        public class MockGrpcClient : Grpc.Core.ClientBase<MockGrpcClient>
        {
            public MockGrpcClient() : base() { }
            protected override MockGrpcClient NewInstance(ClientBase.ClientBaseConfiguration configuration) => new MockGrpcClient();
        }

        public class MockAsyncStreamingCall<T> : AsyncServerStreamingCall<T> where T : class, IMessage<T>, new()
        {
            private readonly int _count;
            private readonly int _interval;

            public MockAsyncStreamingCall(int count, int interval)
                : base(null, null, null, null, null)
            {
                _count = count;
                _interval = interval;
            }

            public override IAsyncStreamReader<T> ResponseStream => new MockStreamReader<T>(_count, _interval);

            private class MockStreamReader<TMessage> : IAsyncStreamReader<TMessage> 
                where TMessage : class, IMessage<TMessage>, new()
            {
                private int _current;
                private readonly int _count;
                private readonly int _interval;

                public MockStreamReader(int count, int interval)
                {
                    _count = count;
                    _interval = interval;
                    _current = 0;
                }

                public TMessage Current { get; private set; }

                public async Task<bool> MoveNext(CancellationToken cancellationToken)
                {
                    if (_current >= _count)
                        return false;

                    await Task.Delay(_interval, cancellationToken);

                    _current++;
                    
                    if (typeof(TMessage) == typeof(ExampleStreamResponse))
                    {
                        var response = new ExampleStreamResponse
                        {
                            SequenceNumber = _current,
                            Message = $"Stream message {_current} of {_count}",
                            Timestamp = Google.Protobuf.WellKnownTypes.Timestamp.FromDateTime(
                                DateTime.UtcNow.ToUniversalTime())
                        };
                        
                        Current = response as TMessage;
                    }
                    else
                    {
                        Current = new TMessage();
                    }

                    return true;
                }
            }
        }

        #endregion
    }
} 