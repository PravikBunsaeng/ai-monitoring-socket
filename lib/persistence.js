'use strict';

const db = require('./db');

const VIOLATION_TYPES = new Set([
  'NO_FACE', 'MULTIPLE_FACE', 'LOOK_AWAY', 'PHONE_DETECTED', 'BOOK_DETECTED',
  'MULTIPLE_PERSON_AI', 'TAB_SWITCH', 'TAB_LEAVE', 'TAB_TIMEOUT',
  'FULLSCREEN_EXIT', 'FOCUS_LOST', 'COPY', 'PASTE', 'CUT', 'RIGHT_CLICK',
  'WINDOW_RESIZE', 'AI_DETECTION', 'VIOLATION', 'EXAM_TERMINATED', 'LECTURER_TERMINATED'
]);

function behaviorRisk(type) {
  const t = String(type || '').toUpperCase();
  if (['TAB_TIMEOUT', 'EXAM_TERMINATED', 'LECTURER_TERMINATED', 'FULLSCREEN_EXIT', 'TAB_SWITCH', 'PHONE_DETECTED', 'MULTIPLE_FACE'].includes(t)) {
    return 'high';
  }
  if (['COPY', 'PASTE', 'CUT', 'LOOK_AWAY', 'NO_FACE', 'FOCUS_LOST', 'TAB_LEAVE'].includes(t)) {
    return 'medium';
  }
  return 'low';
}

function riskFor(type, severity) {
  if (severity === 'critical' || ['TAB_TIMEOUT', 'EXAM_TERMINATED'].includes(type)) return 'danger';
  if (severity === 'high' || behaviorRisk(type) === 'high') return 'danger';
  if (behaviorRisk(type) === 'medium') return 'warning';
  return 'warning';
}

function severityFor(type) {
  const r = behaviorRisk(type);
  if (r === 'high') return 'high';
  if (r === 'medium') return 'medium';
  return 'low';
}

const logDedupeCache = new Map();
const LOG_DEDUPE_MS = 4000;
const CONNECTION_LOG_TYPES = new Set(['STUDENT_JOIN', 'STREAM_READY', 'CONNECTED', 'DISCONNECTED', 'RECONNECTED']);

function shouldDedupeLog(monitoringId, eventType, eventValue) {
  const ev = String(eventType || '').toUpperCase();
  const now = Date.now();
  if (ev === 'TAB_WARNING') return true;
  if (ev === 'ANSWER_TYPING') {
    const typingKey = `${monitoringId}:ANSWER_TYPING:${String(eventValue || '').slice(0, 40)}`;
    const tPrev = logDedupeCache.get(typingKey) || 0;
    if (now - tPrev < 1200) return true;
    logDedupeCache.set(typingKey, now);
    return false;
  }
  const windowMs = CONNECTION_LOG_TYPES.has(ev) ? 5000 : LOG_DEDUPE_MS;
  const key = `${monitoringId}:${ev}:${String(eventValue || '').slice(0, 80)}`;
  const prev = logDedupeCache.get(key) || 0;
  if (now - prev < windowMs) return true;
  logDedupeCache.set(key, now);
  if (logDedupeCache.size > 8000) {
    const cutoff = now - 60000;
    for (const [k, ts] of logDedupeCache) {
      if (ts < cutoff) logDedupeCache.delete(k);
    }
  }
  return false;
}

async function insertMonitoringLog(monitoringId, eventType, eventValue, snapshotPath = null) {
  const ev = String(eventType).slice(0, 50);
  const val = eventValue != null ? String(eventValue).slice(0, 255) : null;
  if (shouldDedupeLog(monitoringId, ev, val)) return;
  try {
    await db.query(
      `INSERT INTO ai_monitoring_logs (monitoring_id, event_type, event_value, snapshot_path)
       VALUES (?, ?, ?, ?)`,
      [monitoringId, ev, val, snapshotPath]
    );
  } catch (err) {
    console.error('[persist] ai_monitoring_logs', err.message);
  }
}

async function logConnection(monitoringId, studentId, socketId, eventType) {
  await insertMonitoringLog(monitoringId, eventType, socketId || null);
}

async function logActivity(monitoringId, studentId, testId, type, value, details) {
  let payload = value || '';
  if (details) {
    try {
      const j = JSON.stringify(details);
      if (j.length <= 200) payload = j;
    } catch (_e) {}
  }
  await insertMonitoringLog(monitoringId, type, payload);
}

async function logViolation(monitoringId, studentId, testId, type, message, meta) {
  if (!VIOLATION_TYPES.has(type) && !String(type).includes('VIOLATION')) {
    return;
  }
  const risk = behaviorRisk(type);
  try {
    const recent = await db.query(
      `SELECT log_id FROM behavior_logs
       WHERE monitoring_id = ? AND behavior_type = ?
         AND timestamp >= DATE_SUB(NOW(), INTERVAL 4 SECOND)
       LIMIT 1`,
      [monitoringId, type]
    );
    if (recent.length > 0) {
      return;
    }
    await db.query(
      `INSERT INTO behavior_logs (monitoring_id, behavior_type, timestamp, risk_level)
       VALUES (?, ?, NOW(), ?)`,
      [monitoringId, type, risk]
    );
    if (message) {
      await insertMonitoringLog(monitoringId, type, String(message).slice(0, 255));
    }
  } catch (err) {
    console.error('[persist] behavior_logs', err.message);
  }
}

async function logChatMessage(monitoringId, testId, studentId, lecturerId, senderRole, body) {
  const eventType = senderRole === 'lecturer' ? 'CHAT_LECTURER' : 'CHAT_STUDENT';
  await insertMonitoringLog(monitoringId, eventType, String(body).slice(0, 255), lecturerId || null);
}

async function logBroadcastMessage(testId, lecturerId, body) {
  const msg = String(body).slice(0, 255);
  try {
    const rows = await db.query(
      `SELECT monitoring_id FROM ai_monitoring
       WHERE test_id = ? AND status = 'running'`,
      [testId]
    );
    for (const row of rows) {
      await insertMonitoringLog(row.monitoring_id, 'CHAT_BROADCAST', msg, lecturerId || null);
    }
    return rows.length;
  } catch (err) {
    console.error('[persist] broadcast', err.message);
    return 0;
  }
}

async function logBroadcastToMonitoring(monitoringId, lecturerId, body) {
  await insertMonitoringLog(monitoringId, 'CHAT_BROADCAST', String(body).slice(0, 255), lecturerId || null);
}

async function terminateMonitoring(monitoringId, reason) {
  try {
    // Older DB enum may not include `blocked_pending_rejoin`.
    // Verify persisted status after update; if not accepted, fallback to `stopped`.
    await db.query(
      `UPDATE ai_monitoring SET status = 'blocked_pending_rejoin', end_time = NOW() WHERE monitoring_id = ?`,
      [monitoringId]
    );

    const verify = await db.query(
      `SELECT status FROM ai_monitoring WHERE monitoring_id = ? LIMIT 1`,
      [monitoringId]
    );
    const persisted = String(verify?.[0]?.status ?? '').toLowerCase();
    if (persisted !== 'blocked_pending_rejoin') {
      await db.query(
        `UPDATE ai_monitoring SET status = 'stopped', end_time = NOW() WHERE monitoring_id = ?`,
        [monitoringId]
      );
    }

    await insertMonitoringLog(monitoringId, 'EXAM_TERMINATED', reason);
  } catch (err) {
    console.error('[persist] terminate', err.message);
  }
}

async function allowRejoin(monitoringId) {
  try {
    await db.query(
      `UPDATE ai_monitoring SET status = 'running', end_time = NULL WHERE monitoring_id = ? AND status IN ('blocked_pending_rejoin', 'terminated', 'blocked', 'stopped', '')`,
      [monitoringId]
    );
    await insertMonitoringLog(monitoringId, 'REJOIN_APPROVED', 'Lecturer allowed rejoin');
  } catch (err) {
    console.error('[persist] rejoin', err.message);
  }
}

async function completeMonitoring(monitoringId, submissionId) {
  try {
    if (submissionId) {
      await db.query(
        `UPDATE ai_monitoring SET status = 'completed', end_time = NOW(), submission_id = ? WHERE monitoring_id = ?`,
        [submissionId, monitoringId]
      );
    } else {
      await db.query(
        `UPDATE ai_monitoring SET status = 'completed', end_time = NOW() WHERE monitoring_id = ?`,
        [monitoringId]
      );
    }

    // Verify persisted status; fallback for older enum.
    const verify = await db.query(
      `SELECT status FROM ai_monitoring WHERE monitoring_id = ? LIMIT 1`,
      [monitoringId]
    );
    const persisted = String(verify?.[0]?.status ?? '').toLowerCase();
    if (persisted !== 'completed') {
      if (submissionId) {
        await db.query(
          `UPDATE ai_monitoring SET status = 'terminated', end_time = NOW(), submission_id = ? WHERE monitoring_id = ?`,
          [submissionId, monitoringId]
        );
      } else {
        await db.query(
          `UPDATE ai_monitoring SET status = 'terminated', end_time = NOW() WHERE monitoring_id = ?`,
          [monitoringId]
        );
      }
    }

    await insertMonitoringLog(monitoringId, 'SUBMISSION_COMPLETED', 'Exam auto-submitted on timeout');
  } catch (err) {
    console.error('[persist] complete', err.message);
  }
}

module.exports = {
  logConnection,
  logActivity,
  logViolation,
  logChatMessage,
  logBroadcastMessage,
  logBroadcastToMonitoring,
  terminateMonitoring,
  allowRejoin,
  completeMonitoring,
  severityFor,
  riskFor
};
