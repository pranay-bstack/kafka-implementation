import net from "net";
import { Worker } from "worker_threads";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import KafkaAdminClient from "./metadata-manager.js";
import { API_TYPES, KAFKA_CLIENTS } from "./contansts.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class KafkaBroker {
  constructor(port = 9092, numWorkers = 4) {
    this.port = port;
    this.server = null;
    this.requestQueue = [];
    this.workers = [];
    this.numWorkers = numWorkers;
    this.connections = new Map();
    this.nextWorkerId = 0;
    this.partitionOffsets = new Map();
    this.adminClientInstance = new KafkaAdminClient();
  }

  start() {
    this.startWorkers();

    this.server = net.createServer((socket) => {
      this.handleConnection(socket);
    });

    this.server.listen(this.port, () => {
      console.log(`Kafka Broker listening on port ${this.port}`);
      console.log(`Workers: ${this.numWorkers}`);
    });
  }

  startWorkers() {
    const workerPath = join(__dirname, "io-worker.js");

    for (let i = 0; i < this.numWorkers; i++) {
      const worker = new Worker(workerPath, { workerData: { threadId: i } });

      worker.on("message", (response) => {
        this.handleWorkerResponse(response);
      });

      worker.on("error", (err) => {
        console.error(`Worker ${i} error:`, err);
      });

      this.workers.push(worker);
      console.log(`Worker ${i} started`);
    }
  }

  handleConnection(socket) {
    const connectionId = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`New connection bisect: ${connectionId}`);
    let buffer = Buffer.alloc(0);
    this.connections.set(connectionId, socket);

    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      // Simple framing: Look for newline-delimited JSON
      let newlineIndex;
      while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);

        try {
          const request = JSON.parse(line.toString());
          request.connectionId = connectionId;
          console.log(`[${connectionId}] Request:`, request);
          if (API_TYPES.PRODUCE) {
            const partition = request.partition || 0;
            const offset = this.partitionOffsets.get(partition) || 0;
            request.offset = offset;
            this.partitionOffsets.set(partition, offset + 1);
          }
          this.requestQueue.push(request);
          console.log(`Queue size: ${this.requestQueue.length}`);

          const worker = this.workers[this.nextWorkerId];
          worker.postMessage(request);
          this.nextWorkerId = (this.nextWorkerId + 1) % this.numWorkers;
        } catch (err) {
          console.error(`[${connectionId}] Parse error:`, err.message);
          this.sendError(socket, err.message);
          // this.stats.errors++;
        }
      }
    });

    socket.on("end", () => {
      console.log(`[${connectionId}] Connection closed`);
      this.connections.delete(connectionId);
    });

    socket.on("error", (err) => {
      console.error(`[${connectionId}] Error:`, err.message);
      this.connections.delete(connectionId);
    });
  }

  handleWorkerResponse(response) {
    console.log("[Main] Worker response:", response);

    const socket = this.connections.get(response.connectionId);

    if (socket) {
      if (response.status === "error") {
        this.sendError(socket, response.message || "Unknown error");
      } else {
        // Success response
        socket.write(JSON.stringify(response.data || {"status": "ok"}) + "\n");
      }
    } else {
      console.error("Socket not found for:", response.connectionId);
    }

    this.requestQueue.shift();
  }

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
}

const broker = new KafkaBroker(9092, 4);
broker.start();
