using Grpc.Net.Client;
using HubClient.Core.Resilience;
using System;
using System.Net.Http;
using System.Threading.Tasks;

namespace HubClient.Core
{
    /// <summary>
    /// Baseline implementation of gRPC connection management that uses standard
    /// GrpcChannel.ForAddress with default settings
    /// </summary>
    public class BaselineGrpcConnectionManager : IGrpcConnectionManager
    {
        private readonly GrpcChannel _channel;
        private readonly string _serverEndpoint;
        private readonly Lazy<IGrpcResiliencePolicy> _defaultResiliencePolicy;
        private bool _disposed;

        /// <summary>
        /// Creates a new instance of BaselineGrpcConnectionManager
        /// </summary>
        /// <param name="serverEndpoint">The server endpoint to connect to</param>
        public BaselineGrpcConnectionManager(string serverEndpoint)
        {
            _serverEndpoint = serverEndpoint ?? throw new ArgumentNullException(nameof(serverEndpoint));
            
            // Create the standard default channel
            _channel = GrpcChannel.ForAddress(_serverEndpoint);
            
            // Create a lazy-loaded default resilience policy
            _defaultResiliencePolicy = new Lazy<IGrpcResiliencePolicy>(() => 
                new PollyGrpcResiliencePolicy());
        }

        /// <summary>
        /// Gets the gRPC channel to use for communication with the server
        /// </summary>
        public GrpcChannel Channel => _channel;

        /// <summary>
        /// Creates a client of the specified type using the connection
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <returns>A new instance of the client</returns>
        public T CreateClient<T>() where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(BaselineGrpcConnectionManager));

            // Create client using reflection since we don't know the exact type at compile time
            var clientType = typeof(T);
            var constructor = clientType.GetConstructor(new[] { typeof(GrpcChannel) });

            if (constructor == null)
                throw new ArgumentException($"Type {clientType.Name} does not have a constructor that takes a GrpcChannel parameter");

            return (T)constructor.Invoke(new object[] { _channel });
        }
        
        /// <summary>
        /// Creates a resilient client of the specified type using the connection and resilience policy
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <param name="resiliencePolicy">The resilience policy to use (null to use default)</param>
        /// <returns>A new resilient client wrapper</returns>
        public ResilientGrpcClient<T> CreateResilientClient<T>(IGrpcResiliencePolicy? resiliencePolicy = null) where T : class
        {
            if (_disposed)
                throw new ObjectDisposedException(nameof(BaselineGrpcConnectionManager));
                
            // Create the underlying client
            var client = CreateClient<T>();
            
            // Create a resilient wrapper with the specified or default policy
            return new ResilientGrpcClient<T>(
                client, 
                resiliencePolicy ?? _defaultResiliencePolicy.Value);
        }

        /// <summary>
        /// Releases all resources used by the connection manager
        /// </summary>
        public void Dispose()
        {
            if (!_disposed)
            {
                _channel.Dispose();
                _disposed = true;
            }
            
            GC.SuppressFinalize(this);
        }
    }
} 