class Logger {
  constructor(component = "", showTimestamp = true) {
    this.showTimestamp = showTimestamp;
    this.component = component;
  }

  log = (...message) => {
    const timestamp = new Date().toISOString();
    console.log(`${this.showTimestamp ? [timestamp] + " " : ""}[${this.component}]`, ...message);
  };

  error = (...message) => {
    const timestamp = new Date().toISOString();
    console.log(`${this.showTimestamp ? [timestamp] + " " : ""}[${this.component}] Error`, ...message);
  };

  static log(...message) {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}]`, ...message);
  }

  static error(...message) {
    const timestamp = new Date().toISOString();
    console.error(`[${timestamp}] ERROR: ${message}`);
  }
}

export default Logger;
