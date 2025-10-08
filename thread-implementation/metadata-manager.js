import fs from "fs/promises";
import Logger from "./logger.js";
import { ensureDataDir } from "./utils.js";
import { LOG_DIRECTORY } from "./config.js";
import { ADMIN_COMMAND_TYPES } from "./contansts.js";

const logger = new Logger("MetadataManager", false);

/**
 * MetadataManager - Read-only metadata cache manager
 * Responsibilities:
 * - Load and replay WAL on startup
 * - Maintain in-memory cache of topics
 * - Expose read-only methods (NO writes)
 */
class MetadataManager {
  constructor() {
    this.cache = {};
    this.metaWalPath = LOG_DIRECTORY.METADATA;
    ensureDataDir(this.metaWalPath);
  }

  async initialize() {
    await this.replayWAL();
  }

  replayWAL = async () => {
    logger.log("Loading metadata cache from", this.metaWalPath);
    try {
      const data = await fs.readFile(this.metaWalPath, "utf-8");
      const lines = data.split("\n").filter((line) => line.trim() !== "");
      
      for (const line of lines) {
        try {
          const record = JSON.parse(line);
          this.applyRecord(record);
        } catch (err) {
          logger.error("Failed to parse WAL line:", line, err);
        }
      }
      logger.log("Metadata cache loaded:", Object.keys(this.cache).length, "topics");
    } catch (err) {
      if (err.code === 'ENOENT') {
        logger.log("No existing metadata WAL found, starting fresh");
      } else {
        throw err;
      }
    }
  };

  applyRecord = (record) => {
    switch (record.type) {
      case ADMIN_COMMAND_TYPES.CREATE_TOPIC:
      case ADMIN_COMMAND_TYPES.EDIT_PARTITIONS:
        this.cache[record.name] = {
          partitions: record.partitions,
          createdBy: record.createdBy,
          createdAt: new Date(record.timestamp).toLocaleString(),
          timestamp: record.timestamp,
        };
        break;
      default:
        logger.log("Unknown admin record type:", record.type);
    }
  };

  // Read-only operations (served from main thread cache)
  listTopics = () => {
    return Object.keys(this.cache).map((topic) => ({
      name: topic,
      partitions: this.cache[topic].partitions,
      createdBy: this.cache[topic].createdBy || null,
      createdAt: this.cache[topic].createdAt || null,
    }));
  };

  checkTopicExists = (topic) => {
    return !!this.cache[topic];
  };

  getTopicMetadata = (topic) => {
    return this.cache[topic] || null;
  };

  updateCache = (record) => {
    this.applyRecord(record);
  };
}

export default MetadataManager;