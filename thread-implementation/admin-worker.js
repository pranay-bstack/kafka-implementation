import { parentPort } from "worker_threads";
import fs from "fs/promises";
import Logger from "./logger.js";
import { LOG_DIRECTORY } from "./config.js";
import { ADMIN_COMMAND_TYPES } from "./contansts.js";

const logger = new Logger("AdminWorker", false);
const metaWalPath = LOG_DIRECTORY.METADATA;

/**
 * AdminWorker - Single-threaded metadata write handler
 * Responsibilities:
 * - Handle all metadata mutations (create topic, edit partitions)
 * - Validate requests before writing
 * - Write to metadata WAL
 * - Notify main thread of cache updates
 */

// In-memory cache for validation (synced with WAL)
const metadataCache = {};

// Replay WAL on startup to build validation cache
const replayWAL = async () => {
  try {
    const data = await fs.readFile(metaWalPath, "utf-8");
    const lines = data.split("\n").filter((line) => line.trim() !== "");
    
    for (const line of lines) {
      try {
        const record = JSON.parse(line);
        applyToCache(record);
      } catch (err) {
        logger.error("Failed to parse WAL line:", err);
      }
    }
    logger.log("Admin worker cache loaded:", Object.keys(metadataCache).length, "topics");
  } catch (err) {
    if (err.code !== 'ENOENT') {
      logger.error("Error replaying WAL:", err);
    }
  }
};

const applyToCache = (record) => {
  switch (record.type) {
    case ADMIN_COMMAND_TYPES.CREATE_TOPIC:
    case ADMIN_COMMAND_TYPES.EDIT_PARTITIONS:
      metadataCache[record.name] = {
        partitions: record.partitions,
        createdBy: record.createdBy,
        timestamp: record.timestamp,
      };
      break;
  }
};

const createTopic = async (request) => {
  const { topic, partitions = 1, connectionId } = request;

  // Validation
  if (metadataCache[topic]) {
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
  await fs.appendFile(metaWalPath, JSON.stringify(record) + "\n");
  
  // Update local cache
  applyToCache(record);

  logger.log(`Topic ${topic} created with ${partitions} partitions`);

  return {
    success: true,
    topic,
    partitions,
    record, // Send record back to main thread for cache update
  };
};

const editTopicPartition = async (request) => {
  const { topic, newPartitionCount, connectionId } = request;

  // Validation
  if (!metadataCache[topic]) {
    throw new Error(`Topic ${topic} does not exist`);
  }

  if (!newPartitionCount || isNaN(newPartitionCount)) {
    throw new Error(`Invalid new partition count: ${newPartitionCount}`);
  }

  if (newPartitionCount <= metadataCache[topic].partitions) {
    throw new Error(
      `New partition count must be greater than existing count (${metadataCache[topic].partitions})`
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
  await fs.appendFile(metaWalPath, JSON.stringify(record) + "\n");
  
  // Update local cache
  applyToCache(record);

  logger.log(`Topic ${topic} partitions updated to ${newPartitionCount}`);

  return {
    success: true,
    topic,
    partitions: newPartitionCount,
    record, // Send record back to main thread for cache update
  };
};

// Initialize
replayWAL();

// Handle messages from main thread
parentPort.on("message", async (request) => {
  try {
    let result;

    switch (request.type) {
      case ADMIN_COMMAND_TYPES.CREATE_TOPIC:
        result = await createTopic(request);
        break;
      
      case ADMIN_COMMAND_TYPES.EDIT_PARTITIONS:
        result = await editTopicPartition(request);
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
    logger.error("Admin operation failed:", err.message);
    
    // Send error response back to main thread
    parentPort.postMessage({
      status: "error",
      connectionId: request.connectionId,
      message: err.message,
    });
  }
});