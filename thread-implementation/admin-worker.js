import { parentPort } from "worker_threads";
import fs from "fs/promises";
import Logger from "./logger.js";
import { LOG_DIRECTORY } from "./config.js";
import { ADMIN_COMMAND_TYPES } from "./contansts.js";

/**
 * AdminWorker - Single-threaded metadata write handler
 * Responsibilities:
 * - Handle all metadata mutations (create topic, edit partitions)
 * - Validate requests before writing
 * - Write to metadata WAL
 * - Notify main thread of cache updates
 */
class AdminWorker {
  constructor() {
    this.logger = new Logger("AdminWorker", false);
    this.metaWalPath = LOG_DIRECTORY.METADATA;
    this.metadataCache = {};
    
    this.initialize();
  }

  async initialize() {
    await this.replayWAL();
    this.setupMessageHandler();
  }

  /**
   * Replay WAL on startup to build validation cache
   */
  async replayWAL() {
    try {
      const data = await fs.readFile(this.metaWalPath, "utf-8");
      const lines = data.split("\n").filter((line) => line.trim() !== "");
      
      for (const line of lines) {
        try {
          const record = JSON.parse(line);
          this.applyToCache(record);
        } catch (err) {
          this.logger.error("Failed to parse WAL line:", err);
        }
      }
      this.logger.log("Admin worker cache loaded:", Object.keys(this.metadataCache).length, "topics");
    } catch (err) {
      if (err.code !== 'ENOENT') {
        this.logger.error("Error replaying WAL:", err);
      }
    }
  }

  applyToCache(record) {
    switch (record.type) {
      case ADMIN_COMMAND_TYPES.CREATE_TOPIC:
      case ADMIN_COMMAND_TYPES.EDIT_PARTITIONS:
        this.metadataCache[record.name] = {
          partitions: record.partitions,
          createdBy: record.createdBy,
          timestamp: record.timestamp,
        };
        break;
    }
  }

  async createTopic(request) {
    const { topic, partitions = 1, connectionId } = request;

    // Validation
    if (this.metadataCache[topic]) {
      throw new Error(`Topic ${topic} already exists`);
    }

    if (!partitions || isNaN(partitions) || partitions < 1) {
      throw new Error(`Invalid partition count: ${partitions}`);
    }

    // Create record
    const record = {
      type: ADMIN_COMMAND_TYPES.CREATE_TOPIC,
      createdBy: connectionId,
      timestamp: Date.now(),
      name: topic,
      partitions,
    };

    // Write to WAL
    await fs.appendFile(this.metaWalPath, JSON.stringify(record) + "\n");
    
    // Update local cache
    this.applyToCache(record);

    this.logger.log(`Topic ${topic} created with ${partitions} partitions`);

    return {
      success: true,
      topic,
      partitions,
      record, // Send record back to main thread for cache update
    };
  }

  async editTopicPartition(request) {
    const { topic, newPartitionCount, connectionId } = request;

    // Validation
    if (!this.metadataCache[topic]) {
      throw new Error(`Topic ${topic} does not exist`);
    }

    if (!newPartitionCount || isNaN(newPartitionCount)) {
      throw new Error(`Invalid new partition count: ${newPartitionCount}`);
    }

    if (newPartitionCount <= this.metadataCache[topic].partitions) {
      throw new Error(
        `New partition count must be greater than existing count (${this.metadataCache[topic].partitions})`
      );
    }

    // Create record
    const record = {
      type: ADMIN_COMMAND_TYPES.EDIT_PARTITIONS,
      createdBy: connectionId,
      timestamp: Date.now(),
      name: topic,
      partitions: newPartitionCount,
    };

    // Write to WAL
    await fs.appendFile(this.metaWalPath, JSON.stringify(record) + "\n");
    
    // Update local cache
    this.applyToCache(record);

    this.logger.log(`Topic ${topic} partitions updated to ${newPartitionCount}`);

    return {
      success: true,
      topic,
      partitions: newPartitionCount,
      record, // Send record back to main thread for cache update
    };
  }

  async handleMessage(request) {
    try {
      let result;

      switch (request.apiKey) {
        case ADMIN_COMMAND_TYPES.CREATE_TOPIC:
          result = await this.createTopic(request);
          break;
        
        case ADMIN_COMMAND_TYPES.EDIT_PARTITIONS:
          result = await this.editTopicPartition(request);
          break;
        
        default:
          throw new Error(`Unknown admin command: ${request.type}`);
      }

      // Send success response back to main thread
      parentPort.postMessage({
        status: "success",
        connectionId: request.connectionId,
        data: result,
      });
    } catch (err) {
      this.logger.error("Admin operation failed:", err.message);
      
      // Send error response back to main thread
      parentPort.postMessage({
        status: "error",
        connectionId: request.connectionId,
        message: err.message,
      });
    }
  }

  setupMessageHandler() {
    parentPort.on("message", (request) => this.handleMessage(request));
  }
}

// Initialize the worker
new AdminWorker();