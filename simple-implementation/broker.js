// broker.js - Just accepting connections
import crypto from "crypto";
import fs from "fs/promises";
import { createServer } from "net";
import path from "path";

class KafkaBroker {
  constructor(config = {}) {
    this.port = config.port || 9092;
    this.walPath = config.walPath || "./data/wal.log";
    this.offset = 0;

    this.stats = {
      totalRequests: 0,
      successfulWrites: 0,
      errors: 0,
      activeConnections: 0,
      startTime: Date.now(),
    };

    this.ensureDataDir();
  }

  async ensureDataDir() {
    const dir = path.dirname(this.walPath);
    try {
      await fs.mkdir(dir, { recursive: true });
    //   console.log(`Data directory ready: ${dir}`);
    } catch (err) {
      console.error("Failed to create data directory:", err);
    }
  }

  start() {
    // Start network server (main event loop handles this)
    this.server = createServer((socket) => {
      this.handleConnection(socket);
    });

    this.server.listen(this.port, () => {
      console.log("=================================");
      console.log(`Simple Kafka Broker Started`);
      console.log(`Listening on port: ${this.port}`);
        console.log(`WAL location: ${this.walPath}`);
      console.log("=================================\n");
    });

    this.server.on("error", (err) => {
      console.error("Server error:", err);
    });

    // Stats reporting
    this.startStatsReporting();
  }

  handleConnection(socket) {
    const clientId = `${socket.remoteAddress}:${socket.remotePort}`;
    this.stats.activeConnections++;

    console.log(`New connection: ${clientId}`);
    let buffer = Buffer.alloc(0);

    // Handle incoming data
    socket.on("data", async (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      // Simple framing: Look for newline-delimited JSON
      let newlineIndex;
      while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);

        try {
          const request = JSON.parse(line.toString());
          await this.handleRequest(socket, request, clientId);
        } catch (err) {
          console.error(`[${clientId}] Parse error:`, err.message);
          this.sendError(socket, "Invalid JSON format");
          this.stats.errors++;
        }
      }
    });

    socket.on("end", () => {
      console.log(`[${new Date().toISOString()}] Connection closed: ${clientId}`);
      this.stats.activeConnections--;
    });

    socket.on("error", (err) => {
      console.error(`[${clientId}] Socket error:`, err.message);
      this.stats.activeConnections--;
    });
  }

  async handleRequest(socket, request, clientId) {
    this.stats.totalRequests++;
    const startTime = Date.now();

    console.log(`[${clientId}] Received request:`, request);

    try {
      if (!request.topic || request.value === undefined) {
        throw new Error("Missing required fields: topic and value");
      }

      const walEntry = {
        offset: this.offset++,
        timestamp: Date.now(),
        topic: request.topic,
        partition: request.partition || 0,
        key: request.key || null,
        value: request.value,
      };

      // CRITICAL: This is async but main thread handles it
      await this.writeToWAL(walEntry);

      this.stats.successfulWrites++;
      const duration = Date.now() - startTime;

      const response = {
        status: "success",
        offset: walEntry.offset,
        topic: walEntry.topic,
        partition: walEntry.partition,
        timestamp: walEntry.timestamp,
        latency: duration,
      };

      this.sendResponse(socket, response);

      console.log(`[${clientId}] ✓ Written (offset: ${walEntry.offset}, latency: ${duration}ms)`);
    } catch (err) {
      console.error(`[${clientId}] Request handling error:`, err.message);
      this.sendError(socket, "Internal server error");
      this.stats.errors++;
    }
  }

  async writeToWAL(entry) {
    // Simple approach: append to file
    // Note: No fsync here - data goes to OS cache
    const line = JSON.stringify(entry) + "\n";

    try {
      await fs.appendFile(this.walPath, line);
      this.cpuHeavyTask(line);
      // At this point, data is in OS cache (not necessarily on disk!)
    } catch (err) {
      throw new Error(`WAL write failed: ${err.message}`);
    }
  }

  cpuHeavyTask = (data) => {
    const str = typeof data === "string" ? data : JSON.stringify(data);
    let hash = 0;

    // Intentionally CPU-intensive loop
    for (let i = 0; i < str.length * 1000; i++) {
      hash = (hash << 5) - hash + str.charCodeAt(i % str.length);
      hash = hash & hash; // Convert to 32-bit integer
    }

    let result = "";
    const iterations = 100; // Simulate multiple encryption rounds

    for (let round = 0; round < iterations; round++) {
      for (let i = 0; i < data.length; i++) {
        // XOR with round number (simplified encryption simulation)
        const encrypted = data.charCodeAt(i) ^ round % 256;
        result += String.fromCharCode(encrypted);
      }
    }

    let checksum = 0xffffffff;

    for (let i = 0; i < data.length; i++) {
      const byte = data.charCodeAt(i);
      checksum ^= byte;

      // Polynomial division simulation (CPU intensive)
      for (let j = 0; j < 8; j++) {
        if (checksum & 1) {
          checksum = (checksum >>> 1) ^ 0xedb88320;
        } else {
          checksum = checksum >>> 1;
        }
      }
    }

    const start = Date.now();
    let count = 0;

    while (Date.now() - start < 100) {
      crypto
        .createHash("sha256")
        .update("some random data " + count)
        .digest("hex");
      count++;
    }

    console.log(`Performed ${count} SHA256 hashes in 5s`);
  };

  sendResponse(socket, response) {
    try {
      socket.write(JSON.stringify(response) + "\n");
    } catch (err) {
      console.error("Failed to send response:", err.message);
    }
  }

  sendError(socket, message) {
    const error = {
      status: "error",
      message: message,
    };
    this.sendResponse(socket, error);
  }

  stop() {
    if (this.server) {
      this.server.close();
      console.log("Broker stopped");
    }
  }

  startStatsReporting() {
    setInterval(() => {
      const uptime = Math.floor((Date.now() - this.stats.startTime) / 1000);
      const throughput = this.stats.successfulWrites / (uptime || 1);
      
      console.log('\n--- Broker Stats ---');
      console.log(`Uptime: ${uptime}s`);
      console.log(`Active connections: ${this.stats.activeConnections}`);
      console.log(`Total requests: ${this.stats.totalRequests}`);
      console.log(`Successful writes: ${this.stats.successfulWrites}`);
      console.log(`Errors: ${this.stats.errors}`);
      console.log(`Throughput: ${throughput.toFixed(2)} msg/sec`);
      console.log(`Current offset: ${this.offset}`);
      console.log('------------------- \n');
    }, 5000);
  }
}
if (import.meta.url === `file://${process.argv[1]}`) {
  const broker = new KafkaBroker({
    port: 9092,
    walPath: "./data/wal.log",
  });

  broker.start();

  // Graceful shutdown
  process.on("SIGINT", () => {
    console.log("\nShutting down gracefully...");
    broker.stop();
    process.exit(0);
  });
}

export default KafkaBroker;
