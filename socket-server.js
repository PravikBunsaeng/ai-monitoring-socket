'use strict';

/**
 * Real-time proctoring Socket.IO server
 * Port 4000 (default) — start: cd realtime && npm install && npm start
 */

const http = require('http');
const express = require('express');
const cors = require('cors');
const { Server } = require('socket.io');
const config = require('./config');
const DEMO_MODE = true;

async function safeDb(action, fallback = null) {
  if (DEMO_MODE) return fallback;
  try {
    return await action();
  } catch (err) {
    console.error('[safeDb]', err.message);
    return fallback;
  }
}
const auth = require('./lib/auth');
const rateLimit = require('./lib/rateLimit');
const persist = require('./lib/persistence');
const db = require('./lib/db');

const app = express();
app.use(cors({
  origin: '*',
  methods: ['GET', 'POST'],
  credentials: false
}));
app.get('/health', (_req, res) => res.json({ ok: true, service: 'proctoring-socket', ts: Date.now() }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: {
    origin: '*',
    methods: ['GET', 'POST'],
    credentials: false
  },
  transports: ['websocket', 'polling'],
  pingInterval: 20000,
  pingTimeout: 25000,
  maxHttpBufferSize: 1e6
});

/** @type {Map<string, object>} socket.id -> session */
const sessions = new Map();
/** monitoring_id -> latest student socket id */
const studentSocketByMonitoring = new Map();
/** monitoring_id -> Set<lecturer socket ids> */
const lecturersWatching = new Map();
/** duplicate join guard: monitoring_id -> socket.id */
const studentJoinLock = new Map();
/** monitoring_id -> last request_stream_ready broadcast (ms) */
const streamReadyCooldown = new Map();

const LECTURER_ROOM = 'lecturers';
const examRoom = (testId) => `exam:${testId}`;
const monitoringRoom = (mid) => `monitor:${mid}`;

function emitToLecturers(event, payload) {
  io.to(LECTURER_ROOM).emit(event, payload);
  if (payload.test_id) {
    io.to(examRoom(payload.test_id)).emit(event, payload);
  }
  if (payload.monitoring_id) {
    io.to(monitoringRoom(payload.monitoring_id)).emit(event, payload);
  }
}

function mapViolationLabel(type) {
  const map = {
    NO_FACE: 'No face detected',
    MULTIPLE_FACE: 'Multiple faces',
    LOOK_AWAY: 'Looking away',
    PHONE_DETECTED: 'Phone detected',
    TAB_SWITCH: 'Tab switching',
    TAB_TIMEOUT: 'Tab away timeout',
    FULLSCREEN_EXIT: 'Fullscreen exit',
    FOCUS_LOST: 'Browser blur',
    COPY: 'Copy attempt',
    PASTE: 'Paste attempt',
    CUT: 'Cut attempt',
    AI_DETECTION: 'AI detection',
    EXAM_TERMINATED: 'Exam terminated'
  };
  return map[type] || type;
}

function normalizeExamEvent(ev) {
  if (!ev || !ev.event_type) return null;
  const type = String(ev.event_type).toUpperCase();
  const payload = ev.payload || {};
  return {
    event_type: type,
    monitoring_id: Number(ev.monitoring_id),
    test_id: Number(ev.test_id),
    student_id: ev.student_id,
    monitoring_token: ev.monitoring_token,
    timestamp: ev.timestamp || new Date().toISOString(),
    payload
  };
}

async function handleStudentRealtimeEvent(socket, ev) {
  const sess = sessions.get(socket.id);
  if (!sess || sess.role !== 'student') return;

  const mid = Number(ev.monitoring_id);
  if (mid !== sess.monitoring_id) return;

  const type = ev.event_type;
  const testId = Number(ev.test_id) || sess.test_id;
  const studentId = ev.student_id || sess.student_id;
  const payload = ev.payload || {};

  if (type === 'TAB_WARNING') {
    return;
  }

  if (['VIOLATION', 'AI_DETECTION', 'COPY', 'PASTE', 'CUT', 'TAB_SWITCH', 'TAB_TIMEOUT',
    'FULLSCREEN_EXIT', 'FOCUS_LOST', 'NO_FACE', 'MULTIPLE_FACE', 'LOOK_AWAY', 'PHONE_DETECTED',
    'RIGHT_CLICK', 'EXAM_TERMINATED'].includes(type)) {
    const msg = payload.message || payload.details || mapViolationLabel(type);
    await persist.logViolation(mid, studentId, testId, type, msg, payload);
    emitToLecturers('violation_alert', {
      monitoring_id: mid,
      student_id: studentId,
      test_id: testId,
      violation_type: type,
      message: msg,
      risk_level: persist.riskFor(type, persist.severityFor(type)),
      timestamp: ev.timestamp
    });
    emitToLecturers('exam_event', ev);
    return;
  }

  if (type === 'STREAM_READY') {
    studentSocketByMonitoring.set(mid, socket.id);
    sess.stream_ready = true;
    await persist.logConnection(mid, studentId, socket.id, 'STREAM_READY', payload);
    if (!sess.join_announced) {
      sess.join_announced = true;
      emitToLecturers('student_joined', {
        monitoring_id: mid,
        student_id: studentId,
        test_id: testId,
        socket_id: socket.id,
        timestamp: ev.timestamp
      });
    }
    emitToLecturers('exam_event', ev);
    return;
  }

  if (type === 'ANSWER_UPDATED' || type === 'ANSWER_TYPING') {
    const qKey = payload.question_key != null ? String(payload.question_key) : '';
    const activityLabel = payload.question_text
      ? (payload.selected_label || payload.option_label
        ? `${payload.question_text} · ${payload.selected_label || payload.option_label}`
        : payload.question_text)
      : qKey;
    await persist.logActivity(mid, studentId, testId,
      type === 'ANSWER_UPDATED' ? 'QUESTION_ANSWERED' : 'ANSWER_TYPING',
      activityLabel.slice(0, 200), payload);
    emitToLecturers('student_progress', {
      monitoring_id: mid,
      student_id: studentId,
      test_id: testId,
      answered_count: payload.answered_count,
      active_question: qKey,
      event_type: type,
      payload,
      timestamp: ev.timestamp
    });
    return;
  }

  if (type === 'TAB_RETURN') {
    await persist.logActivity(mid, studentId, testId, 'TAB_RETURN', null, payload);
  }

  emitToLecturers('live_activity', {
    monitoring_id: mid,
    student_id: studentId,
    activity_type: type,
    activity_value: payload.phase || payload.message || '',
    details: payload,
    timestamp: ev.timestamp
  });
  emitToLecturers('exam_event', ev);
}

io.on('connection', (socket) => {
  console.log('[socket] connect', socket.id);

  socket.emit('server_ack', { socket_id: socket.id, ts: Date.now() });

  /* ---------- Legacy join (dashboard + take_test) ---------- */
  socket.on('join_exam', async (data = {}) => {
    const role = String(data.role || '').toLowerCase();
    if (role === 'lecturer') {
      const lecturerId = auth.sanitizeText(data.lecturer_id || data.user_no, 30);
      const token = data.auth_token || data.token || '';
      const sessionId = data.session_id || '';
      if (lecturerId && token && !auth.verifyLecturerToken(lecturerId, sessionId, token)) {
        if (!data.skip_auth) {
          socket.emit('auth_error', { message: 'Invalid lecturer token' });
          return;
        }
      }
      sessions.set(socket.id, { role: 'lecturer', lecturer_id: lecturerId || 'anonymous' });
      socket.join(LECTURER_ROOM);
      if (data.test_id) socket.join(examRoom(data.test_id));
      return;
    }
    if (role === 'student') {
      const monitoringId = Number(data.monitoring_id);
      const testId = Number(data.test_id);
      const studentId = auth.sanitizeText(data.student_id, 30);
      const token = data.monitoring_token || '';
      if (!monitoringId || !studentId || !testId) return;
      if (token && !auth.verifyStudentToken(studentId, testId, monitoringId, token)) {
        socket.emit('auth_error', { message: 'Invalid student token' });
        return;
      }

      const prev = studentJoinLock.get(monitoringId);
      if (prev && prev !== socket.id) {
        const oldSock = io.sockets.sockets.get(prev);
        if (oldSock) {
          oldSock.emit('session_replaced', { monitoring_id: monitoringId });
          oldSock.disconnect(true);
        }
      }
      studentJoinLock.set(monitoringId, socket.id);
      studentSocketByMonitoring.set(monitoringId, socket.id);

      sessions.set(socket.id, {
        role: 'student',
        student_id: studentId,
        test_id: testId,
        monitoring_id: monitoringId,
        monitoring_token: data.monitoring_token || '',
        join_announced: true
      });
      socket.join(examRoom(testId));
      socket.join(monitoringRoom(monitoringId));

      await safeDb(() => persist.logConnection(monitoringId, studentId, socket.id, 'STUDENT_JOIN', { test_id: testId }));
      emitToLecturers('student_joined', {
        monitoring_id: monitoringId,
        student_id: studentId,
        test_id: testId,
        socket_id: socket.id,
        timestamp: new Date().toISOString()
      });
    }
  });

  /* ---------- Student events (new API) ---------- */
  socket.on('student_join', async (data = {}) => {
    if (!rateLimit.allow(`sj:${socket.id}`, 5, 60000)) return;
    const monitoringId = Number(data.monitoring_id);
    const testId = Number(data.test_id);
    const studentId = auth.sanitizeText(data.student_id, 30);
    const token = data.monitoring_token || '';
    const sessionId = data.session_id || '';

    if (!monitoringId || !studentId || !testId) return;
    if (token && !auth.verifyStudentToken(studentId, testId, monitoringId, token)) {
      socket.emit('auth_error', { message: 'Invalid student token' });
      return;
    }
    if (!DEMO_MODE && !(await auth.studentOwnsMonitoring(studentId, monitoringId))) {
      socket.emit('auth_error', { message: 'Monitoring session mismatch' });
      return;
    }

    const prev = studentJoinLock.get(monitoringId);
    if (prev && prev !== socket.id) {
      const oldSock = io.sockets.sockets.get(prev);
      if (oldSock) oldSock.disconnect(true);
    }
    studentJoinLock.set(monitoringId, socket.id);
    studentSocketByMonitoring.set(monitoringId, socket.id);

    const existing = sessions.get(socket.id);
    if (existing?.role === 'student' && existing.monitoring_id === monitoringId && existing.join_announced) {
      studentSocketByMonitoring.set(monitoringId, socket.id);
      return;
    }
    sessions.set(socket.id, {
      role: 'student',
      student_id: studentId,
      test_id: testId,
      monitoring_id: monitoringId,
      monitoring_token: token,
      join_announced: true
    });
    socket.join(examRoom(testId));
    socket.join(monitoringRoom(monitoringId));
    await safeDb(() => persist.logConnection(monitoringId, studentId, socket.id, 'STUDENT_JOIN', data));
    emitToLecturers('student_joined', {
      monitoring_id: monitoringId,
      student_id: studentId,
      test_id: testId,
      socket_id: socket.id,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('stream_ready', async (data = {}) => {
    if (!rateLimit.allow(`sr:${socket.id}`, 20, 60000)) return;
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'student') return;
    const mid = Number(data.monitoring_id) || sess.monitoring_id;
    studentSocketByMonitoring.set(mid, socket.id);
    sess.stream_ready = true;
    await persist.logConnection(mid, sess.student_id, socket.id, 'STREAM_READY', data);
    if (!sess.join_announced) {
      sess.join_announced = true;
      emitToLecturers('student_joined', {
        monitoring_id: mid,
        student_id: sess.student_id,
        test_id: sess.test_id,
        socket_id: socket.id,
        stream_ready: true,
        timestamp: new Date().toISOString()
      });
    }
    socket.to(LECTURER_ROOM).emit('exam_event', {
      event_type: 'STREAM_READY',
      monitoring_id: mid,
      test_id: sess.test_id,
      student_id: sess.student_id,
      payload: { socket_id: socket.id, ...(data.payload || {}) },
      timestamp: new Date().toISOString()
    });
  });

  socket.on('heartbeat', (data = {}) => {
    if (!rateLimit.allow(`hb:${socket.id}`, config.rateLimits.heartbeat.max, config.rateLimits.heartbeat.windowMs)) return;
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'student') return;
    sess.last_heartbeat = Date.now();
    socket.emit('heartbeat_ack', { ts: Date.now() });
  });

  socket.on('question_answered', async (data = {}) => {
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'student') return;
    const q = auth.sanitizeText(data.question_key, 20);
    await safeDb(() => persist.logActivity(sess.monitoring_id, sess.student_id, sess.test_id, 'QUESTION_ANSWERED', q, data));
    const ts = new Date().toISOString();
    emitToLecturers('student_progress', {
      monitoring_id: sess.monitoring_id,
      student_id: sess.student_id,
      test_id: sess.test_id,
      active_question: q,
      event_type: 'QUESTION_ANSWERED',
      payload: data,
      timestamp: ts
    });
    emitToLecturers('live_activity', {
      monitoring_id: sess.monitoring_id,
      student_id: sess.student_id,
      activity_type: 'QUESTION_ANSWERED',
      activity_value: q,
      details: data,
      timestamp: ts
    });
  });

  socket.on('activity_update', async (data = {}) => {
    if (!rateLimit.allow(`act:${socket.id}`, 80, 60000)) return;
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'student') return;
    const type = auth.sanitizeText(data.activity_type, 64);
    await persist.logActivity(sess.monitoring_id, sess.student_id, sess.test_id, type, data.activity_value, data.details);
    emitToLecturers('live_activity', {
      monitoring_id: sess.monitoring_id,
      student_id: sess.student_id,
      activity_type: type,
      activity_value: data.activity_value,
      details: data.details,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('violation_detected', async (data = {}) => {
    if (!rateLimit.allow(`vio:${socket.id}`, 60, 60000)) return;
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'student') return;
    const type = auth.sanitizeText(data.violation_type || data.type, 64).toUpperCase();
    const msg = auth.sanitizeText(data.message, 300);
    await safeDb(() => persist.logViolation(sess.monitoring_id, sess.student_id, sess.test_id, type, msg, data.meta || data));
    emitToLecturers('violation_alert', {
      monitoring_id: sess.monitoring_id,
      student_id: sess.student_id,
      test_id: sess.test_id,
      violation_type: type,
      message: msg,
      risk_level: persist.riskFor(type, persist.severityFor(type)),
      timestamp: new Date().toISOString()
    });
  });

  /* ---------- Legacy exam_event ---------- */
  socket.on('exam_event', async (raw) => {
    if (!rateLimit.allow(`ev:${socket.id}`, config.rateLimits.examEvent.max, config.rateLimits.examEvent.windowMs)) return;
    const ev = normalizeExamEvent(raw);
    if (!ev) return;
    await handleStudentRealtimeEvent(socket, ev);
  });

  /* ---------- WebRTC signaling ---------- */
  socket.on('webrtc_signal', (signal = {}) => {
    if (!rateLimit.allow(`rtc:${socket.id}`, config.rateLimits.webrtcSignal.max, config.rateLimits.webrtcSignal.windowMs)) return;
    const to = signal.to_socket_id;
    if (!to) return;
    const sess = sessions.get(socket.id);
    const payload = { ...signal, from_socket_id: socket.id };
    if (sess && sess.role === 'lecturer' && signal.monitoring_id) {
      const studentSid = studentSocketByMonitoring.get(Number(signal.monitoring_id));
      if (studentSid) {
        io.to(studentSid).emit('webrtc_signal', payload);
        return;
      }
    }
    io.to(to).emit('webrtc_signal', payload);
  });

  /* ---------- Lecturer controls ---------- */
  socket.on('lecturer_auth', async (data = {}) => {
    const lecturerId = auth.sanitizeText(data.lecturer_id, 30);
    const token = data.auth_token || '';
    const sessionId = data.session_id || '';
    if (!lecturerId || !auth.verifyLecturerToken(lecturerId, sessionId, token)) {
      socket.emit('auth_error', { message: 'Lecturer authentication failed' });
      return;
    }
    const prev = sessions.get(socket.id);
    sessions.set(socket.id, {
      role: 'lecturer',
      lecturer_id: lecturerId,
      watched_test_id: prev?.watched_test_id || Number(data.test_id) || 0
    });
    socket.join(LECTURER_ROOM);
    if (data.test_id) {
      sessions.get(socket.id).watched_test_id = Number(data.test_id);
      socket.join(examRoom(data.test_id));
    }
    socket.emit('lecturer_ready', { lecturer_id: lecturerId });
  });

  socket.on('lecturer_watch_test', (data = {}) => {
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const testId = Number(data.test_id) || 0;
    const prev = Number(sess.watched_test_id) || 0;
    if (prev > 0 && prev !== testId) {
      socket.leave(examRoom(prev));
    }
    sess.watched_test_id = testId;
    if (testId > 0) socket.join(examRoom(testId));
  });

  socket.on('watch_monitoring', async (data = {}) => {
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const mid = Number(data.monitoring_id);
    if (!DEMO_MODE && !(await auth.lecturerOwnsMonitoring(sess.lecturer_id, mid))) return;
    socket.join(monitoringRoom(mid));
    let set = lecturersWatching.get(mid);
    if (!set) {
      set = new Set();
      lecturersWatching.set(mid, set);
    }
    set.add(socket.id);
    const studentSid = studentSocketByMonitoring.get(mid);
    if (studentSid) {
      io.to(studentSid).emit('request_stream_ready', { monitoring_id: mid, lecturer_socket_id: socket.id });
    }
  });

  socket.on('request_all_stream_ready', () => {
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const lecturerSocketId = socket.id;
    studentSocketByMonitoring.forEach((studentSid, mid) => {
      const perLecturerKey = `${mid}:${lecturerSocketId}`;
      const last = streamReadyCooldown.get(perLecturerKey) || 0;
      const now = Date.now();
      if (now - last < 4000) return;
      streamReadyCooldown.set(perLecturerKey, now);
      io.to(studentSid).emit('request_stream_ready', {
        monitoring_id: mid,
        lecturer_socket_id: lecturerSocketId
      });
      io.to(studentSid).emit('resend_stream_ready', {
        monitoring_id: mid,
        lecturer_socket_id: lecturerSocketId
      });
    });
  });

  socket.on('request_student_logs', async (data = {}, ack) => {
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const mid = Number(data.monitoring_id);
    if (!DEMO_MODE && !(await auth.lecturerOwnsMonitoring(sess.lecturer_id, mid))) return;
    try {
      const activities = await db.query(
        `SELECT event_type AS activity_type, event_value AS activity_value, created_at
         FROM ai_monitoring_logs WHERE monitoring_id = ?
           AND event_type NOT IN ('CHAT_LECTURER', 'CHAT_STUDENT')
         ORDER BY log_id DESC LIMIT 50`,
        [mid]
      );
      const violations = await db.query(
        `SELECT behavior_type AS violation_type, risk_level AS severity, timestamp AS created_at
         FROM behavior_logs WHERE monitoring_id = ? ORDER BY log_id DESC LIMIT 50`,
        [mid]
      );
      const result = { ok: true, activities, violations };
      if (typeof ack === 'function') ack(result);
      else socket.emit('student_logs', { monitoring_id: mid, ...result });
    } catch (err) {
      if (typeof ack === 'function') ack({ ok: false, message: err.message });
    }
  });

  socket.on('send_warning', async (data = {}) => {
    if (!rateLimit.allow(`warn:${socket.id}`, 30, 60000)) return;
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const mid = Number(data.monitoring_id);
    if (!DEMO_MODE && !(await auth.lecturerOwnsMonitoring(sess.lecturer_id, mid))) return;
    const msg = auth.sanitizeText(data.message || 'Please focus on your exam.', 300);
    const studentSid = studentSocketByMonitoring.get(mid);
    const rows = await safeDb(
      () => db.query('SELECT student_id, test_id FROM ai_monitoring WHERE monitoring_id = ? LIMIT 1', [mid]),
      [{ student_id: 'student', test_id: 0 }]
    );
const { student_id: studentId, test_id: testId } = rows[0];
    await persist.logActivity(mid, studentId, testId, 'LECTURER_WARNING', msg, { lecturer_id: sess.lecturer_id });
    if (studentSid) {
      io.to(studentSid).emit('lecturer_warning', { monitoring_id: mid, message: msg });
    }
    emitToLecturers('live_activity', {
      monitoring_id: mid,
      student_id: studentId,
      activity_type: 'LECTURER_WARNING',
      activity_value: msg,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('terminate_student', async (data = {}) => {
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const mid = Number(data.monitoring_id);
    if (!DEMO_MODE && !(await auth.lecturerOwnsMonitoring(sess.lecturer_id, mid))) return;
    const reason = auth.sanitizeText(data.reason || 'LECTURER_TERMINATED', 120);
    const rows = await safeDb(
      () => db.query('SELECT student_id, test_id FROM ai_monitoring WHERE monitoring_id = ? LIMIT 1', [mid]),
      [{ student_id: 'student', test_id: 0 }]
    );
const { student_id: studentId, test_id: testId } = rows[0];
    await safeDb(() => persist.terminateMonitoring(mid, reason));
    await persist.logViolation(mid, studentId, testId, 'LECTURER_TERMINATED', reason, { lecturer_id: sess.lecturer_id });
    const studentSid = studentSocketByMonitoring.get(mid);
    if (studentSid) {
      io.to(studentSid).emit('exam_terminated', { monitoring_id: mid, reason });
    }
    emitToLecturers('student_terminated', {
      monitoring_id: mid,
      student_id: studentId,
      test_id: testId,
      reason,
      timestamp: new Date().toISOString()
    });
  });

  socket.on('allow_rejoin', async (data = {}) => {
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const mid = Number(data.monitoring_id);
    if (!DEMO_MODE && !(await auth.lecturerOwnsMonitoring(sess.lecturer_id, mid))) return;
    await safeDb(() => persist.allowRejoin(mid));
    const rows = await db.query('SELECT student_id, test_id FROM ai_monitoring WHERE monitoring_id = ? LIMIT 1', [mid]);
    if (!rows.length) return;
    const testId = rows[0].test_id;
    const rejoinPayload = {
      monitoring_id: mid,
      test_id: testId,
      student_id: rows[0].student_id,
      timestamp: new Date().toISOString()
    };
    const studentSid = studentSocketByMonitoring.get(mid);
    if (studentSid) {
      io.to(studentSid).emit('exam_rejoin_allowed', rejoinPayload);
    }
    io.to(monitoringRoom(mid)).emit('exam_rejoin_allowed', rejoinPayload);
    if (testId) {
      io.to(examRoom(testId)).emit('exam_rejoin_allowed', rejoinPayload);
    }
    emitToLecturers('live_activity', {
      monitoring_id: mid,
      student_id: rows[0].student_id,
      activity_type: 'REJOIN_APPROVED',
      timestamp: new Date().toISOString()
    });
  });

  socket.on('broadcast_message', async (data = {}) => {
    if (!rateLimit.allow(`bcast:${socket.id}`, 20, 60000)) return;
    const sess = sessions.get(socket.id);
    if (!sess || sess.role !== 'lecturer') return;
    const testId = Number(data.test_id) || 0;
    const body = auth.sanitizeText(data.message, 500);
    if (!body) return;
    const lecturerId = sess.lecturer_id;
    const payload = {
      message: body,
      test_id: testId,
      sender_role: 'lecturer',
      broadcast: true,
      created_at: new Date().toISOString()
    };

    if (testId > 0) {
      if (!DEMO_MODE && !(await auth.lecturerOwnsTest(lecturerId, testId))) return;
      await safeDb(() => persist.logBroadcastMessage(testId, lecturerId, body));
      io.to(examRoom(testId)).emit('lecturer_broadcast', payload);
      emitToLecturers('live_activity', {
        monitoring_id: 0,
        student_id: '',
        activity_type: 'BROADCAST',
        activity_value: `[Test ${testId}] ${body}`,
        timestamp: payload.created_at
      });
      return;
    }

    for (const [sid, stu] of sessions) {
      if (!stu || stu.role !== 'student' || !stu.monitoring_id) continue;
      const mid = Number(stu.monitoring_id);
      if (!(await auth.lecturerOwnsMonitoring(lecturerId, mid))) continue;
      await safeDb(() => persist.logBroadcastToMonitoring(mid, lecturerId, body));
      io.to(sid).emit('lecturer_broadcast', {
        ...payload,
        test_id: stu.test_id,
        monitoring_id: mid
      });
    }
    emitToLecturers('live_activity', {
      monitoring_id: 0,
      student_id: '',
      activity_type: 'BROADCAST',
      activity_value: `[All sessions] ${body}`,
      timestamp: payload.created_at
    });
  });

  socket.on('private_message', async (data = {}) => {
    if (!rateLimit.allow(`chat:${socket.id}`, config.rateLimits.chat.max, config.rateLimits.chat.windowMs)) return;
    const sess = sessions.get(socket.id);
    if (!sess) return;
    const mid = Number(data.monitoring_id);
    const body = auth.sanitizeText(data.message, 500);
    if (!body) return;

    if (sess.role === 'lecturer') {
      if (!sess.lecturer_id || (!DEMO_MODE && !(await auth.lecturerOwnsMonitoring(sess.lecturer_id, mid)))) {
        socket.emit('chat_error', { message: 'Not authorized for this student session' });
        return;
      }
      const rows = await safeDb(
        () => db.query('SELECT student_id, test_id FROM ai_monitoring WHERE monitoring_id = ? LIMIT 1', [mid]),
        [{ student_id: 'student', test_id: 0 }]
      );
      const { student_id: studentId, test_id: testId } = rows[0];
      await safeDb(() => persist.logChatMessage(mid, testId, studentId, sess.lecturer_id, 'lecturer', body));
      const studentSid = studentSocketByMonitoring.get(mid);
      const chatPayload = {
        monitoring_id: mid,
        sender_role: 'lecturer',
        message: body,
        message_id: data.message_id || `chat-${mid}-${Date.now()}`,
        created_at: new Date().toISOString()
      };
      if (studentSid) {
        io.to(studentSid).emit('private_message', chatPayload);
      }
      socket.emit('private_message_sent', { monitoring_id: mid, message: body });
      return;
    }

    if (sess.role === 'student' && mid === sess.monitoring_id) {
      emitToLecturers('private_message', {
        monitoring_id: mid,
        student_id: sess.student_id,
        sender_role: 'student',
        message: body,
        message_id: data.message_id || `stu-${mid}-${Date.now()}`,
        created_at: new Date().toISOString()
      });
      
      socket.emit('private_message_sent', {
        monitoring_id: mid,
        message: body
      });
      return;
    }
  });

  socket.on('disconnect', () => {
    const sess = sessions.get(socket.id);
    if (!sess) return;

    if (sess.role === 'student' && sess.monitoring_id) {
      const locked = studentJoinLock.get(sess.monitoring_id);
      if (locked === socket.id) {
        studentJoinLock.delete(sess.monitoring_id);
      }
      if (studentSocketByMonitoring.get(sess.monitoring_id) === socket.id) {
        studentSocketByMonitoring.delete(sess.monitoring_id);
      }
      safeDb(() => persist.logConnection(sess.monitoring_id, sess.student_id, socket.id, 'DISCONNECTED', {}));
      emitToLecturers('student_disconnected', {
        monitoring_id: sess.monitoring_id,
        student_id: sess.student_id,
        test_id: sess.test_id,
        timestamp: new Date().toISOString()
      });
    }

    if (sess.role === 'lecturer') {
      lecturersWatching.forEach((set, mid) => {
        set.delete(socket.id);
        if (set.size === 0) lecturersWatching.delete(mid);
      });
    }

    sessions.delete(socket.id);
    console.log('[socket] disconnect', socket.id, sess.role);
  });
});

server.listen(config.port, '0.0.0.0', () => {
  console.log(`[proctoring] Socket.IO listening on port ${config.port}`);
});
