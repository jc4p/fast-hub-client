# Hub Service Simulator Client

A minimal viable gRPC client for the Hub Service Simulator, built with Node.js and runnable with Bun.

## Prerequisites

- [Bun](https://bun.sh/) installed on your system
- Hub Service Simulator running locally on port 5293

## Installation

```bash
# Install dependencies
bun install
```

## Running the Client

```bash
# Run the client using Bun
bun start
```

This will:
1. Connect to the Hub Service Simulator at http://localhost:5293
2. Call the `getUserDataByFid` method with FID=24
3. Display the response data in a structured format

## Expected Output

If successful, you should see the user data for FID=24, including:
- Display Name
- Profile Picture URL
- Bio
- User URL (if available)

## Troubleshooting

- Make sure the Hub Service Simulator is running and accessible
- Check that the proto files are correctly placed in the `./proto` directory
- Verify that the server is listening on port 5293 (check server logs for the actual port) 