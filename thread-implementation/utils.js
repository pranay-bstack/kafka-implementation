import fs from "fs";
import path from "path";
import Logger from "./logger.js";

export const ensureDataDir = (walPath) => {
  const dir = path.dirname(walPath);
  try {
    if (!fs.existsSync(dir)) {
      fs.mkdir(dir, { recursive: true });
      Logger.log(`${dir} directory ready`);
    }

    if (!fs.existsSync(walPath)) {
      fs.writeFileSync(walPath, '');
      Logger.log(`${walPath} file created`);
    }
  } catch (err) {
    Logger.error("Failed to create data directory:", err);
  }
};
