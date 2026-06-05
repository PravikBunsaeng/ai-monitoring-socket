'use strict';

const crypto = require('crypto');
const config = require('../config');
const db = require('./db');

function hmac(data) {
  return crypto.createHmac('sha256', config.secret).update(data).digest('hex');
}

function verifyLecturerToken(lecturerId, sessionId, token) {
  if (!lecturerId || !token) return false;
  const expected = hmac(`lecturer|${lecturerId}|${sessionId || ''}`);
  return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(String(token)));
}

function verifyStudentToken(studentId, testId, monitoringId, token) {
  if (!studentId || !testId || !monitoringId || !token) return false;
  const expected = hmac(`${studentId}|${testId}|${monitoringId}`);
  try {
    return crypto.timingSafeEqual(Buffer.from(expected), Buffer.from(String(token)));
  } catch (_e) {
    return false;
  }
}

async function lecturerOwnsMonitoring(lecturerId, monitoringId) {
  const rows = await db.query(
    `SELECT am.monitoring_id
     FROM ai_monitoring am
     INNER JOIN tests t ON t.test_id = am.test_id
     LEFT JOIN lecturer_sections ls ON ls.section_id = t.section_id AND ls.lecturer_id = ?
     WHERE am.monitoring_id = ?
       AND (ls.lecturer_id IS NOT NULL OR t.lecturer_id = ?)
     LIMIT 1`,
    [lecturerId, monitoringId, lecturerId]
  );
  return rows.length > 0;
}

async function lecturerOwnsTest(lecturerId, testId) {
  const rows = await db.query(
    `SELECT t.test_id
     FROM tests t
     LEFT JOIN lecturer_sections ls ON ls.section_id = t.section_id AND ls.lecturer_id = ?
     WHERE t.test_id = ?
       AND (ls.lecturer_id IS NOT NULL OR t.lecturer_id = ?)
     LIMIT 1`,
    [lecturerId, testId, lecturerId]
  );
  return rows.length > 0;
}

async function studentOwnsMonitoring(studentId, monitoringId) {
  const rows = await db.query(
    `SELECT monitoring_id FROM ai_monitoring
     WHERE monitoring_id = ? AND student_id = ?
     LIMIT 1`,
    [monitoringId, studentId]
  );
  return rows.length > 0;
}

function sanitizeText(input, maxLen = 500) {
  if (input == null) return '';
  return String(input)
    .replace(/[\x00-\x08\x0B\x0C\x0E-\x1F]/g, '')
    .trim()
    .slice(0, maxLen);
}

module.exports = {
  verifyLecturerToken,
  verifyStudentToken,
  lecturerOwnsMonitoring,
  lecturerOwnsTest,
  studentOwnsMonitoring,
  sanitizeText
};
