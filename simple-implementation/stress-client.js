// load-tester.js
import { createConnection } from "net";

class LoadTester {
  constructor(config = {}) {
    this.host = config.host || "localhost";
    this.port = config.port || 9092;
    this.numClients = config.numClients || 10;
    this.messagesPerClient = config.messagesPerClient || 100;
    this.clients = [];

    this.stats = {
      totalSent: 0,
      totalAcked: 0,
      totalErrors: 0,
      latencies: [],
      startTime: null,
      endTime: null,
    };
  }

  async run() {
    console.log("=================================");
    console.log("Load Test Configuration:");
    console.log(`Clients: ${this.numClients}`);
    console.log(`Messages per client: ${this.messagesPerClient}`);
    console.log(`Total messages: ${this.numClients * this.messagesPerClient}`);
    console.log("=================================\n");

    this.stats.startTime = Date.now();

    // Create multiple concurrent clients
    const clientPromises = [];
    for (let i = 0; i < this.numClients; i++) {
      clientPromises.push(this.runClient(i));
    }

    // Wait for all clients to finish
    await Promise.all(clientPromises);

    this.stats.endTime = Date.now();
    this.printResults();
  }

  async runClient(clientId) {
    return new Promise((resolve, reject) => {
      const socket = createConnection({
        host: this.host,
        port: this.port,
      });

      let buffer = Buffer.alloc(0);
      let messagesSent = 0;
      let messagesAcked = 0;
      const sentTimestamps = new Map();

      socket.on("connect", () => {
        console.log(`[Client ${clientId}] Connected`);

        // Send all messages rapidly
        for (let i = 0; i < this.messagesPerClient; i++) {
          const msgId = `${clientId}-${i}`;
          const message = {
            topic: "load-test",
            key: `client-${clientId}`,
            value: `Message from client ${clientId}, number ${i}`,
            msgId: msgId,
          };

          sentTimestamps.set(msgId, Date.now());
          socket.write(JSON.stringify(message) + "\n");
          messagesSent++;
          this.stats.totalSent++;
        }

        console.log(`[Client ${clientId}] Sent ${messagesSent} messages`);
      });

      socket.on("data", (chunk) => {
        buffer = Buffer.concat([buffer, chunk]);

        let newlineIndex;
        while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
          const line = buffer.slice(0, newlineIndex);
          buffer = buffer.slice(newlineIndex + 1);

          try {
            const response = JSON.parse(line.toString());

            if (response.status === "success") {
              messagesAcked++;
              this.stats.totalAcked++;

              // Calculate latency if we can track it
              if (response.latency) {
                this.stats.latencies.push(response.latency);
              }
            } else {
              this.stats.totalErrors++;
            }

            // Close connection when all messages are acknowledged
            if (messagesAcked === messagesSent) {
              console.log(`[Client ${clientId}] All messages acknowledged`);
              socket.end();
              resolve();
            }
          } catch (err) {
            console.error(`[Client ${clientId}] Parse error:`, err.message);
          }
        }
      });

      socket.on("error", (err) => {
        console.error(`[Client ${clientId}] Error:`, err.message);
        reject(err);
      });

      socket.on("close", () => {
        // If we didn't get all acks, still resolve
        if (messagesAcked < messagesSent) {
          console.log(`[Client ${clientId}] Connection closed with ${messagesSent - messagesAcked} unacknowledged`);
        }
        resolve();
      });
    });
  }

  printResults() {
    const duration = (this.stats.endTime - this.stats.startTime) / 1000;
    const throughput = this.stats.totalAcked / duration;

    // Calculate latency percentiles
    const sortedLatencies = this.stats.latencies.sort((a, b) => a - b);
    const p50 = sortedLatencies[Math.floor(sortedLatencies.length * 0.5)] || 0;
    const p95 = sortedLatencies[Math.floor(sortedLatencies.length * 0.95)] || 0;
    const p99 = sortedLatencies[Math.floor(sortedLatencies.length * 0.99)] || 0;
    const avg = sortedLatencies.reduce((a, b) => a + b, 0) / sortedLatencies.length || 0;

    console.log("\n=================================");
    console.log("LOAD TEST RESULTS");
    console.log("=================================");
    console.log(`Duration: ${duration.toFixed(2)}s`);
    console.log(`Total sent: ${this.stats.totalSent}`);
    console.log(`Total acknowledged: ${this.stats.totalAcked}`);
    console.log(`Total errors: ${this.stats.totalErrors}`);
    console.log(`Success rate: ${((this.stats.totalAcked / this.stats.totalSent) * 100).toFixed(2)}%`);
    console.log(`\nThroughput: ${throughput.toFixed(2)} msg/sec`);
    console.log(`\nLatency (ms):`);
    console.log(`  Average: ${avg.toFixed(2)}`);
    console.log(`  p50: ${p50.toFixed(2)}`);
    console.log(`  p95: ${p95.toFixed(2)}`);
    console.log(`  p99: ${p99.toFixed(2)}`);
    console.log("=================================\n");
  }
}

// Run different test scenarios
async function runTests() {
  console.log("Starting load tests...\n");

  // Test 1: Light load
  console.log("\n### TEST 1: Light Load ###");
  const test1 = new LoadTester({
    numClients: 5,
    messagesPerClient: 20,
  });
  await test1.run();
  await sleep(2000);

  // Test 2: Medium load
  console.log("\n### TEST 2: Medium Load ###");
  const test2 = new LoadTester({
    numClients: 10,
    messagesPerClient: 100,
  });
  await test2.run();
  await sleep(2000);

  // Test 3: Heavy load (This is where we'll see issues!)
  console.log("\n### TEST 3: Heavy Load ###");
  const test3 = new LoadTester({
    numClients: 100,
    messagesPerClient: 1000,
  });
  await test3.run();

  console.log("\n✓ All tests complete!");
  process.exit(0);
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runTests().catch((err) => {
    console.error("Test failed:", err);
    process.exit(1);
  });
}

export default LoadTester;
