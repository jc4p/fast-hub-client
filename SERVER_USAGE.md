# Hub Service Simulator - Usage Guide

This document provides instructions for creating a minimal viable gRPC client to interact with the Hub Service Simulator.

## Server Endpoint

The Hub Service Simulator listens on the following endpoints by default:
- HTTPS: `https://localhost:5001` (recommended for secure connections)
- HTTP: `http://localhost:5000` (for development only)

## Creating a Minimal gRPC Client

Below are examples in different languages showing how to create a simple client that fetches users and casts from the Hub Service Simulator.

### C# Client

#### Prerequisites
- .NET SDK (compatible with .NET 9.0)
- NuGet packages:
  - `Google.Protobuf`
  - `Grpc.Net.Client`
  - `Grpc.Tools`

#### Step 1: Create a new .NET project
```bash
dotnet new console -n HubClientExample
cd HubClientExample
```

#### Step 2: Add required NuGet packages
```bash
dotnet add package Google.Protobuf
dotnet add package Grpc.Net.Client
dotnet add package Grpc.Tools
```

#### Step 3: Create the .proto file
Create a file named `minimal_hub_service.proto` in your project directory with the Minimal Hub Service definition. Typically, this would be provided by the service developers. The file should include definitions for:
- User data structures
- Cast/message structures
- Service methods like `GetUserDataByFid` and `GetCastsByFid`

#### Step 4: Configure project to generate gRPC code
Update your `.csproj` file to include:

```xml
<ItemGroup>
  <Protobuf Include="minimal_hub_service.proto" GrpcServices="Client" />
</ItemGroup>
```

#### Step 5: Implement the client code
Create a simple client that connects to the service and retrieves user and cast data:

```csharp
using System;
using System.Threading.Tasks;
using Grpc.Net.Client;

namespace HubClientExample
{
    class Program
    {
        static async Task Main(string[] args)
        {
            // Create a channel to the Hub Service
            using var channel = GrpcChannel.ForAddress("https://localhost:5001");
            
            // Create a client
            var client = new MinimalHubService.MinimalHubServiceClient(channel);
            
            // Example 1: Get user data by FID (Farcaster ID)
            var userRequest = new GetUserDataByFidRequest { Fid = 1 }; // Replace with actual FID
            var userResponse = await client.GetUserDataByFidAsync(userRequest);
            Console.WriteLine($"User: {userResponse.UserData.DisplayName}");
            
            // Example 2: Get casts by FID
            var castsRequest = new GetCastsByFidRequest
            {
                Fid = 1, // Replace with actual FID
                PageSize = 10,
                PageToken = "" // Empty for first page
            };
            var castsResponse = await client.GetCastsByFidAsync(castsRequest);
            
            Console.WriteLine("Recent casts:");
            foreach (var cast in castsResponse.Casts)
            {
                Console.WriteLine($"- {cast.Text}");
            }
            
            // Example 3: Get a specific cast by ID
            var castRequest = new GetCastRequest { CastId = "123" }; // Replace with actual cast ID
            var castResponse = await client.GetCastAsync(castRequest);
            Console.WriteLine($"Cast content: {castResponse.Cast.Text}");
        }
    }
}
```

### Python Client

#### Prerequisites
- Python 3.7+
- Required packages:
  - `grpcio`
  - `grpcio-tools`
  - `protobuf`

#### Step 1: Install required packages
```bash
pip install grpcio grpcio-tools protobuf
```

#### Step 2: Get the .proto file
Save the `minimal_hub_service.proto` file to your project directory.

#### Step 3: Generate Python gRPC code
```bash
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. minimal_hub_service.proto
```

#### Step 4: Create a minimal client
Create a file named `hub_client.py`:

```python
import grpc
import minimal_hub_service_pb2
import minimal_hub_service_pb2_grpc

def main():
    # Create a secure channel
    channel = grpc.insecure_channel('localhost:5000')  # Use 5001 with SSL
    
    # Create a stub (client)
    stub = minimal_hub_service_pb2_grpc.MinimalHubServiceStub(channel)
    
    # Example 1: Get user data
    user_request = minimal_hub_service_pb2.GetUserDataByFidRequest(fid=1)  # Replace with actual FID
    user_response = stub.GetUserDataByFid(user_request)
    print(f"User: {user_response.user_data.display_name}")
    
    # Example 2: Get casts by user
    casts_request = minimal_hub_service_pb2.GetCastsByFidRequest(
        fid=1,  # Replace with actual FID
        page_size=10,
        page_token=""  # Empty for first page
    )
    casts_response = stub.GetCastsByFid(casts_request)
    
    print("Recent casts:")
    for cast in casts_response.casts:
        print(f"- {cast.text}")
    
    # Example 3: Get a specific cast
    cast_request = minimal_hub_service_pb2.GetCastRequest(cast_id="123")  # Replace with actual cast ID
    cast_response = stub.GetCast(cast_request)
    print(f"Cast content: {cast_response.cast.text}")

if __name__ == "__main__":
    main()
```

### JavaScript/Node.js Client

#### Prerequisites
- Node.js 14+
- Required packages:
  - `@grpc/grpc-js`
  - `@grpc/proto-loader`

#### Step 1: Create a new Node.js project
```bash
mkdir hub-client-js
cd hub-client-js
npm init -y
```

#### Step 2: Install required packages
```bash
npm install @grpc/grpc-js @grpc/proto-loader
```

#### Step 3: Get the .proto file
Save the `minimal_hub_service.proto` file to your project directory.

#### Step 4: Create a minimal client
Create a file named `client.js`:

```javascript
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');

// Load the proto file
const packageDefinition = protoLoader.loadSync('minimal_hub_service.proto', {
  keepCase: true,
  longs: String,
  enums: String,
  defaults: true,
  oneofs: true
});

// Load the package definition
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);
const hubService = protoDescriptor.MinimalHubService;

// Create a client
const client = new hubService('localhost:5000', grpc.credentials.createInsecure());

// Example 1: Get user data
client.getUserDataByFid({ fid: 1 }, (err, response) => {
  if (err) {
    console.error('Error getting user:', err);
    return;
  }
  console.log(`User: ${response.userData.displayName}`);
});

// Example 2: Get casts by user
client.getCastsByFid({ 
  fid: 1, 
  pageSize: 10,
  pageToken: ""  // Empty for first page
}, (err, response) => {
  if (err) {
    console.error('Error getting casts:', err);
    return;
  }
  
  console.log('Recent casts:');
  response.casts.forEach(cast => {
    console.log(`- ${cast.text}`);
  });
});

// Example 3: Get a specific cast
client.getCast({ castId: "123" }, (err, response) => {
  if (err) {
    console.error('Error getting cast:', err);
    return;
  }
  console.log(`Cast content: ${response.cast.text}`);
});
```

## Important Notes

1. **Connection Security**:
   - For production use, always use a secure channel (HTTPS).
   - For development/testing, HTTP may be used for simplicity.

2. **Protocol Buffers**:
   - The actual method signatures and data structures will depend on the `.proto` file definition.
   - Ensure you have the correct and up-to-date `.proto` file from the service developers.

3. **Authentication**:
   - The examples above don't include authentication.
   - In a real-world scenario, you might need to add authentication headers or tokens.

4. **Pagination**:
   - Methods that return collections (like `GetCastsByFid`) typically support pagination.
   - Use the `page_token` from the response to fetch the next page.

## Complete API Reference

Refer to the `.proto` file or the README.md for a complete list of available API methods and their parameters.

Key methods for working with users and casts include:

- `GetUserDataByFid`: Fetch a user's profile by their Farcaster ID (FID)
- `GetCast`: Get a specific cast by its ID
- `GetCastsByFid`: Get all casts by a specific user
- `GetCastsByParent`: Get all replies to a specific cast 