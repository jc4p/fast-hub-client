// Minimal viable gRPC client for the Hub Service Simulator
// This client uses @grpc/grpc-js and @grpc/proto-loader to connect to the service
// and call getUserDataByFid with FID=24

// Import required packages
const grpc = require('@grpc/grpc-js');
const protoLoader = require('@grpc/proto-loader');
const path = require('path');

// Path to proto files
const PROTO_PATH = path.join(__dirname, 'proto');

// Load all the proto files needed for the service
const packageDefinition = protoLoader.loadSync(
  [
    path.join(PROTO_PATH, 'minimal_rpc.proto'),
    path.join(PROTO_PATH, 'minimal_message.proto'),
    path.join(PROTO_PATH, 'minimal_request_response.proto'),
    path.join(PROTO_PATH, 'minimal_hub_event.proto')
  ],
  {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
    includeDirs: [PROTO_PATH]
  }
);

// Load the proto descriptors
const protoDescriptor = grpc.loadPackageDefinition(packageDefinition);

// Get the service definition
const hubService = protoDescriptor.MinimalHubService;

// Server endpoints - update based on server logs
const SERVER_ENDPOINT = 'localhost:5293';

// Use the correct endpoint from server logs
const serverEndpoint = SERVER_ENDPOINT;

// Create a client with the appropriate credentials
// For development, we're using insecure credentials as specified in the README
const client = new hubService(serverEndpoint, grpc.credentials.createInsecure());

// The FID we want to look up
const targetFid = 24;

// Make the request
console.log(`Fetching user data for FID: ${targetFid}...`);

client.getUserDataByFid({ fid: targetFid }, (err, response) => {
  if (err) {
    console.error('Error getting user data:', err);
    process.exit(1);
  }

  console.log('Response received:');
  console.log(JSON.stringify(response, null, 2));

  // If we have messages, let's extract some useful information
  if (response.messages && response.messages.length > 0) {
    console.log('\nUser Data:');
    response.messages.forEach(message => {
      if (message.data && message.data.body && message.data.body.user_data_body) {
        const userData = message.data.body.user_data_body;
        const typeMap = {
          1: 'Profile Picture',
          2: 'Display Name',
          3: 'Bio',
          4: 'URL'
        };
        console.log(`${typeMap[userData.type] || 'Unknown'}: ${userData.value}`);
      }
    });
  } else {
    console.log('No user data found for this FID.');
  }
});

console.log('Request sent. Waiting for response...'); 