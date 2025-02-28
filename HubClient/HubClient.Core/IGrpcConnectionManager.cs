using Grpc.Net.Client;
using HubClient.Core.Resilience;
using System;

namespace HubClient.Core
{
    /// <summary>
    /// Interface for gRPC connection managers that handle connection management
    /// </summary>
    public interface IGrpcConnectionManager : IDisposable
    {
        /// <summary>
        /// Gets the gRPC channel to use for communication with the server
        /// </summary>
        GrpcChannel Channel { get; }

        /// <summary>
        /// Creates a client of the specified type using the connection
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <returns>A new instance of the client</returns>
        T CreateClient<T>() where T : class;
        
        /// <summary>
        /// Creates a resilient client of the specified type using the connection and resilience policy
        /// </summary>
        /// <typeparam name="T">The gRPC client type to create</typeparam>
        /// <param name="resiliencePolicy">The resilience policy to use (null to use default)</param>
        /// <returns>A new resilient client wrapper</returns>
        ResilientGrpcClient<T> CreateResilientClient<T>(IGrpcResiliencePolicy? resiliencePolicy = null) where T : class;
    }
} 