// io-worker.js
import { parentPort, workerData } from "worker_threads";
import fs from "fs/promises";
import { LOG_DIRECTORY } from "./config.js";
import { ensureDataDir } from "./utils.js";
import { API_TYPES, KAFKA_CLIENTS } from "./contansts.js";

class IOWorker {
  constructor() {
    this.walPath = LOG_DIRECTORY.WAL;
    this.offset = 0;
    ensureDataDir(this.walPath);
    this.setupMessageHandler();
    this.workerNumber = workerData.threadId;
  }

  setupMessageHandler() {
    parentPort.on("message", async (request) => {
      await this.processRequest(request);
    });
  }

  async processRequest(request) {
    console.log(`[Worker-${this.workerNumber}] Processing request:`, request.connectionId);
    try {
      let response = null;
      switch (request.apiKey) {
        // case API_TYPES.CREATE_TOPIC:
        //   response = await this.adminClientInstance.createTopic(request);
        //   break;
        // case API_TYPES.EDIT_PARTITIONS:
        //   response = await this.adminClientInstance.editTopicPartition(request);
        //   break;
        // case API_TYPES.LIST_TOPICS:
        //   response = this.adminClientInstance.listTopics();
        //   break;
        case API_TYPES.PRODUCE:
          response = await this.handleProducerRequest(request);
          break;
        default:
          console.error(`[${connectionId}] Unknown apiKey:`, request.apiKey);
      }
      // Send success response back to main thread
      parentPort.postMessage({
        connectionId: request.connectionId,
        data: response,
      });
      console.log(`[Worker-${this.workerNumber}] ✓ Processed request for:`, request.connectionId);
    } catch (err) {
      console.error("[Worker] Error:", err);

      // Send error response
      parentPort.postMessage({
        connectionId: request.connectionId,
        status: "error",
        message: err.message,
      });
    }
  }

  handleProducerRequest = async (request) => {
    // Create WAL entry
    const walEntry = {
      offset: request.offset,
      timestamp: Date.now(),
      connectionId: request.connectionId,
      data: request.value,
      partition: request.partition,
    };

    // Write to WAL
    await this.writeToWAL(walEntry);

    // Send success response back to main thread
    return {
      offset: walEntry.offset,
      timestamp: walEntry.timestamp,
    };
  };

  async writeToWAL(entry) {
    const line = JSON.stringify(entry) + "\n";
    await fs.appendFile(this.walPath, line);
    // Note: No fsync yet - data goes to OS cache
  }
}

// Start the worker
const worker = new IOWorker();
console.log(`[Worker-${workerData.threadId}] Started and ready`);
