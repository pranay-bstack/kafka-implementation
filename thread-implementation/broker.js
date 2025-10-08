import net from "net";
import { Worker } from "worker_threads";
import { fileURLToPath } from "url";
import { dirname, join } from "path";
import MetadataManager from "./metadata-manager.js";
import { API_TYPES, ADMIN_COMMAND_TYPES } from "./contansts.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

class KafkaBroker {
  constructor(port = 9092, numWorkers = 4) {
    this.port = port;
    this.server = null;
    this.requestQueue = [];
    this.ioWorkers = [];
    this.adminWorker = null;
    this.numWorkers = numWorkers;
    this.connections = new Map();
    this.nextWorkerId = 0;
    this.partitionOffsets = new Map();
    
    // Metadata manager (main thread cache)
    this.metadataManager = new MetadataManager();
  }

  async start() {
    // Initialize metadata cache
    await this.metadataManager.initialize();
    
    // Start admin worker (single thread)
    this.startAdminWorker();
    
    // Start IO workers (multiple threads)
    this.startIOWorkers();

    this.server = net.createServer((socket) => {
      this.handleConnection(socket);
    });

    this.server.listen(this.port, () => {
      console.log(`Kafka Broker listening on port ${this.port}`);
      console.log(`IO Workers: ${this.numWorkers}`);
      console.log(`Admin Worker: 1 (dedicated)`);
    });
  }

  startAdminWorker() {
    const adminWorkerPath = join(__dirname, "admin-worker.js");
    this.adminWorker = new Worker(adminWorkerPath);

    this.adminWorker.on("message", (response) => {
      this.handleAdminWorkerResponse(response);
    });

    this.adminWorker.on("error", (err) => {
      console.error("Admin worker error:", err);
    });

    console.log("Admin worker started");
  }

  startIOWorkers() {
    const ioWorkerPath = join(__dirname, "io-worker.js");

    for (let i = 0; i < this.numWorkers; i++) {
      const worker = new Worker(ioWorkerPath, { workerData: { threadId: i } });

      worker.on("message", (response) => {
        this.handleIOWorkerResponse(response);
      });

      worker.on("error", (err) => {
        console.error(`IO Worker ${i} error:`, err);
      });

      this.ioWorkers.push(worker);
      console.log(`IO Worker ${i} started`);
    }
  }

  handleConnection(socket) {
    const connectionId = `${socket.remoteAddress}:${socket.remotePort}`;
    console.log(`New connection: ${connectionId}`);
    let buffer = Buffer.alloc(0);
    this.connections.set(connectionId, socket);

    socket.on("data", (chunk) => {
      buffer = Buffer.concat([buffer, chunk]);

      let newlineIndex;
      while ((newlineIndex = buffer.indexOf("\n")) !== -1) {
        const line = buffer.slice(0, newlineIndex);
        buffer = buffer.slice(newlineIndex + 1);

        try {
          const request = JSON.parse(line.toString());
          request.connectionId = connectionId;
          console.log(`[${connectionId}] Request:`, request);

          this.routeRequest(request, socket);
        } catch (err) {
          console.error(`[${connectionId}] Parse error:`, err.message);
          this.sendError(socket, err.message);
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

  routeRequest(request, socket) {
    
    switch (request.apiKey) {
      // Admin read operations - serve from main thread cache (fast path)
      case API_TYPES.LIST_TOPICS:
        this.handleListTopics(request, socket);
        break;

      // case API_TYPES.DE:
      //   this.handleDescribeTopic(request, socket);
      //   break;

      // Admin write operations - forward to admin worker
      case ADMIN_COMMAND_TYPES.CREATE_TOPIC:
      case ADMIN_COMMAND_TYPES.EDIT_PARTITIONS:
        this.forwardToAdminWorker(request);
        break;

      // Produce/Consume operations - forward to IO workers
      case API_TYPES.PRODUCE:
      case API_TYPES.FETCH:
        this.forwardToIOWorker(request, socket);
        break;

      default:
        this.sendError(socket, `Unknown request type: ${type}`);
    }
  }

  // Fast path: List topics from cache (no thread communication)
  handleListTopics(request, socket) {
    try {
      const topics = this.metadataManager.listTopics();
      this.sendResponse(socket, {
        status: "success",
        topics,
      });
    } catch (err) {
      this.sendError(socket, err.message);
    }
  }

  // Fast path: Describe topic from cache
  handleDescribeTopic(request, socket) {
    try {
      const { topic } = request;
      const metadata = this.metadataManager.getTopicMetadata(topic);
      
      if (!metadata) {
        this.sendError(socket, `Topic ${topic} not found`);
        return;
      }

      this.sendResponse(socket, {
        status: "success",
        topic,
        metadata,
      });
    } catch (err) {
      this.sendError(socket, err.message);
    }
  }

  // Forward admin write operations to dedicated admin worker
  forwardToAdminWorker(request) {
    this.requestQueue.push(request);
    this.adminWorker.postMessage(request);
  }

  // Forward produce/consume to IO workers with metadata validation
  forwardToIOWorker(request, socket) {
    // Pre-validate topic exists (fail fast)
    if (request.topic && !this.metadataManager.checkTopicExists(request.topic)) {
      this.sendError(socket, `Topic ${request.topic} does not exist`);
      return;
    }

    // Get topic metadata and attach to request
    if (request.topic) {
      request.topicMetadata = this.metadataManager.getTopicMetadata(request.topic);
    }

    // Add offset tracking for produce requests
    if (request.type === API_TYPES.PRODUCE) {
      const partition = request.partition || 0;
      const offset = this.partitionOffsets.get(partition) || 0;
      request.offset = offset;
      this.partitionOffsets.set(partition, offset + 1);
    }

    this.requestQueue.push(request);

    // Round-robin to IO workers
    const worker = this.ioWorkers[this.nextWorkerId];
    worker.postMessage(request);
    this.nextWorkerId = (this.nextWorkerId + 1) % this.numWorkers;
  }

  handleAdminWorkerResponse(response) {
    console.log("[Main] Admin worker response:", response);

    const socket = this.connections.get(response.connectionId);

    if (socket) {
      if (response.status === "error") {
        this.sendError(socket, response.message);
      } else {
        // Update main thread cache with the new metadata
        if (response.data && response.data.record) {
          this.metadataManager.updateCache(response.data.record);
        }

        // Send success response to client
        socket.write(JSON.stringify({
          status: "success",
          ...response.data
        }) + "\n");
      }
    } else {
      console.error("Socket not found for:", response.connectionId);
    }

    this.requestQueue.shift();
  }

  handleIOWorkerResponse(response) {
    console.log("[Main] IO worker response:", response);

    const socket = this.connections.get(response.connectionId);

    if (socket) {
      if (response.status === "error") {
        this.sendError(socket, response.message || "Unknown error");
      } else {
        socket.write(JSON.stringify(response.data || { status: "ok" }) + "\n");
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