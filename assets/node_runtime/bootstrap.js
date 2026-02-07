'use strict';

const path = require('path');
const fs = require('fs');
const { createServer } = require('http');

const distPath = process.env.NODE_DIST_PATH
  ? path.resolve(process.env.NODE_DIST_PATH)
  : path.resolve(__dirname, 'dist');

const entryPath = path.join(distPath, 'index.js');
const configPath = path.join(distPath, 'index.config.js');

if (!fs.existsSync(entryPath)) {
  console.error(`[bootstrap] index.js not found: ${entryPath}`);
  process.exit(1);
}

// catDartServerPort: upstream calls catDartServerPort() to get the Dart msg
// callback port. We don't have a Dart msg server, so return 0 to skip.
globalThis.catDartServerPort = () => 0;
globalThis.DB_NAME = process.env.DB_NAME || 'kazumi';

// catServerFactory: upstream manages its own listen() call.
// We only create the server and log when it starts listening.
globalThis.catServerFactory = (handler) => {
  const server = createServer((req, res) => handler(req, res));
  server.on('listening', () => {
    const addr = server.address();
    console.log(`Server listening on http://${addr.address}:${addr.port}`);
  });
  return server;
};

const runtime = require(entryPath);
let config = {};
if (fs.existsSync(configPath)) {
  const loaded = require(configPath);
  config = loaded && loaded.default ? loaded.default : loaded;
}

if (!runtime || typeof runtime.start !== 'function') {
  console.error('[bootstrap] dist/index.js must export start(config)');
  process.exit(1);
}

runtime.start(config);

const shutdown = () => {
  try {
    if (runtime && typeof runtime.stop === 'function') {
      runtime.stop();
    }
  } finally {
    process.exit(0);
  }
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
