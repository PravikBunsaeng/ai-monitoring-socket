'use strict';

module.exports = {
  port: Number(process.env.PROCTOR_PORT || 4000),
  corsOrigin: process.env.PROCTOR_CORS_ORIGIN || true,
  db: {
    host: process.env.DB_HOST || 'localhost',
    port: Number(process.env.DB_PORT || 3307),
    user: process.env.DB_USER || 'root',
    password: process.env.DB_PASSWORD || '',
    database: process.env.DB_NAME || 'ai_online_test_system',
    waitForConnections: true,
    connectionLimit: 12
  },
  secret: process.env.PROCTORING_SOCKET_SECRET || 'ai_exam_monitoring_secret_v1',
  rateLimits: {
    heartbeat: { max: 30, windowMs: 60000 },
    examEvent: { max: 120, windowMs: 60000 },
    webrtcSignal: { max: 400, windowMs: 60000 },
    chat: { max: 40, windowMs: 60000 }
  }
};
