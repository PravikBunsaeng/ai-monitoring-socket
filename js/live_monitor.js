/**
 * Lecturer Live Monitor — real-time grid, alerts, chat, controls.
 */
window.LiveMonitorApp = (function () {
    const RISK_CLASS = {
        normal: 'risk-normal',
        warning: 'risk-warning',
        danger: 'risk-danger',
        gray: 'risk-gray'
    };

    const VIOLATION_LABELS = {
        NO_FACE: 'No face detected',
        MULTIPLE_FACE: 'Multiple faces',
        LOOK_AWAY: 'Looking away',
        PHONE_DETECTED: 'Phone detected',
        TAB_SWITCH: 'Tab switching',
        TAB_TIMEOUT: 'Tab away (terminated)',
        FULLSCREEN_EXIT: 'Fullscreen exit',
        FOCUS_LOST: 'Browser blur',
        COPY: 'Copy attempt',
        PASTE: 'Paste attempt',
        CUT: 'Cut attempt',
        AI_DETECTION: 'AI alert',
        EXAM_TERMINATED: 'Exam terminated',
        LECTURER_TERMINATED: 'Terminated by lecturer'
    };

    const ACTIVITY_LABELS = {
        QUESTION_ANSWERED: 'Answered question',
        ANSWER_UPDATED: 'Updated answer',
        ANSWER_TYPING: 'Typing answer',
        ACTIVE_QUESTION: 'Viewing question',
        CONNECTED: 'Connected',
        DISCONNECTED: 'Disconnected',
        STREAM_READY: 'Camera ready',
        TAB_RETURN: 'Returned to exam',
        TAB_SWITCH: 'Left exam tab',
        TAB_WARNING: 'Tab away warning',
        BROADCAST: 'Broadcast message',
        REJOIN_APPROVED: 'Rejoin approved'
    };

    class App {
        constructor(config) {
            this.config = config;
            this.socket = null;
            this.webrtc = null;
            /** @type {Map<number, object>} */
            this.students = new Map();
            /** @type {Map<number, HTMLElement>} */
            this.tiles = new Map();
            /** @type {Map<number, HTMLVideoElement>} */
            this.videos = new Map();
            this.selectedMid = null;
            this.unreadChat = new Map();
            this._initialized = false;
            this._bootstrapTimer = null;
            this._healthTimer = null;
            this._statsTimer = null;
            this._violationDedupe = new Map();
            this._flashUntil = new Map();
            /** @type {Map<string, number>} */
            this._activityDedupe = new Map();
            /** @type {Map<number, number>} */
            this._joinActivityDedupe = new Map();
            /** @type {Set<string>} */
            this._chatLineKeys = new Set();
        }

        init() {
            if (this._initialized) return;
            this._initialized = true;
            this._cacheDom();
            this._initWebRTC();
            this._initSocket();
            this._bindUi();
            this.bootstrapStudents();
            this._bootstrapTimer = setInterval(() => {
                this.bootstrapStudents();
                this.loadAttendance();
            }, 6000);
            this._healthTimer = setInterval(() => this._healthCheck(), 15000);
            this._streamReadyRequested = false;
            this._statsTimer = setInterval(() => this._updateStats(), 3000);
            this._selectDefaultTestFilter();
            this.loadAttendance();
            if (window.ProctorSound) {
                document.getElementById('enableSoundBtn')?.addEventListener('click', () => ProctorSound.unlock());
            }
        }

        _selectDefaultTestFilter() {
            const sel = this.dom?.filterTest || document.getElementById('filterTestId');
            if (!sel || sel.options.length < 2) return;
            if (!sel.value) {
                sel.selectedIndex = 1;
            }
        }

        _cacheDom() {
            this.dom = {
                grid: document.getElementById('studentGrid'),
                feed: document.getElementById('activityFeed'),
                alerts: document.getElementById('alertBanner'),
                alertPopupHost: document.getElementById('alertPopupHost'),
                onlineCount: document.getElementById('onlineCount'),
                alertCount: document.getElementById('alertCountBadge'),
                removedList: document.getElementById('removedStudentsList'),
                chatModal: document.getElementById('chatModal'),
                chatMessages: document.getElementById('chatMessages'),
                chatInput: document.getElementById('chatInput'),
                chatSendBtn: document.getElementById('chatSendBtn'),
                logsModal: document.getElementById('logsModal'),
                logsBody: document.getElementById('logsModalBody'),
                filterTest: document.getElementById('filterTestId'),
                broadcastInput: document.getElementById('broadcastInput'),
                broadcastSendBtn: document.getElementById('broadcastSendBtn'),
                attendanceCompleted: document.getElementById('attendanceCompletedBody'),
                attendanceNotCompleted: document.getElementById('attendanceNotCompletedBody'),
                attCompletedCount: document.getElementById('attCompletedCount'),
                attNotCompletedCount: document.getElementById('attNotCompletedCount')
            };
        }

        _initWebRTC() {
            const WebRTC = window.LecturerWebRTC;
            this.webrtc = new WebRTC.Manager({
                onRemoteStream: (mid, stream) => this._attachVideo(mid, stream),
                onConnectionState: (mid, state) => this._setTileConnection(mid, state === 'connected'),
                onError: (opts) => {
                    if (window.ProctorToast) ProctorToast.show(opts);
                }
            });
        }

        _initSocket() {
            if (typeof io === 'undefined') {
                console.error('[LiveMonitor] Socket.IO not loaded');
                return;
            }
            this.socket = io(this.config.socketUrl, {
                transports: ['websocket'],
                upgrade: false,
                reconnection: true,
                timeout: 20000
            });

            this.socket.on('connect_error', (err) => {
                console.error('[LECTURER] socket connect_error', err.message);
                if (window.ProctorToast) {
                    ProctorToast.show({
                        title: 'Realtime server unreachable',
                        message: 'Cannot connect to ' + this.config.socketUrl + '. Start: cd realtime && npm start. Use your PC LAN IP on other devices, not localhost.',
                        level: 'danger'
                    });
                }
            });

            this.socket.on('connect', () => {
                console.log('[LECTURER] socket connected', this.socket.id, '→', this.config.socketUrl);
                this.webrtc.setSocket(this.socket, this.socket.id);
                const testId = Number(this.dom.filterTest?.value) || 0;
                this.socket.emit('lecturer_auth', {
                    lecturer_id: this.config.lecturerId,
                    auth_token: this.config.authToken,
                    session_id: this.config.sessionId,
                    test_id: testId || undefined
                });
                this.socket.emit('join_exam', {
                    role: 'lecturer',
                    lecturer_id: this.config.lecturerId,
                    auth_token: this.config.authToken,
                    session_id: this.config.sessionId,
                    test_id: testId || undefined,
                    skip_auth: false
                });
                if (testId > 0) {
                    this.socket.emit('lecturer_watch_test', { test_id: testId });
                }
                if (!this._streamReadyRequested) {
                    this._streamReadyRequested = true;
                    this.socket.emit('request_all_stream_ready');
                }
            });

            this.socket.on('webrtc_signal', (signal) => {
                this.webrtc.handleSignal(signal).catch((err) => console.error('[LECTURER] signal', err));
            });

            this.socket.on('exam_event', (ev) => this._handleExamEvent(ev));
            this.socket.on('violation_alert', (p) => this._handleViolation(p));
            this.socket.on('live_activity', (p) => {
                this._appendActivity(this._enrichActivityPayload(p));
                if (p.activity_type === 'REJOIN_APPROVED') {
                    const mid = Number(p.monitoring_id);
                    if (mid) {
                        this._removeFromRemovedList(mid);
                        const s = this.students.get(mid);
                        if (s) {
                            s.terminated = false;
                            s.status = 'running';
                            s.is_online = false;
                            this._updateTileMeta(mid);
                        }
                    }
                    this._loadRemovedStudents();
                    this.bootstrapStudents();
                }
            });
            this.socket.on('student_progress', (p) => this._handleProgress(p));
            this.socket.on('student_joined', (p) => this._handleStudentJoined(p));
            this.socket.on('student_disconnected', (p) => this._handleDisconnected(p));
            this.socket.on('student_terminated', (p) => this._handleTerminated(p));
            this.socket.on('private_message', (p) => this._handleIncomingChat(p));
        }

        _bindUi() {
            this.dom.filterTest?.addEventListener('change', () => {
                const testId = Number(this.dom.filterTest?.value) || 0;
                if (this.socket?.connected) {
                    this.socket.emit('lecturer_watch_test', { test_id: testId });
                    this.socket.emit('request_all_stream_ready');
                }
                this.bootstrapStudents();
                this.loadAttendance();
                this._loadRemovedStudents();
            });
            document.getElementById('refreshStreamsBtn')?.addEventListener('click', () => {
                this.socket?.emit('request_all_stream_ready');
            });
            document.getElementById('chatSendBtn')?.addEventListener('click', () => this._sendChat());
            this.dom.chatInput?.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this._sendChat();
            });
            document.getElementById('broadcastSendBtn')?.addEventListener('click', () => this._sendBroadcast());
            this.dom.broadcastInput?.addEventListener('keydown', (e) => {
                if (e.key === 'Enter') this._sendBroadcast();
            });
        }

        async bootstrapStudents() {
            try {
                const testId = this.dom.filterTest?.value || '';
                const url = testId
                    ? `get_live_students.php?test_id=${encodeURIComponent(testId)}`
                    : 'get_live_students.php';
                const res = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
                const ct = (res.headers.get('content-type') || '').toLowerCase();
                if (!res.ok) {
                    const errText = ct.includes('json') ? JSON.stringify(await res.json()) : (await res.text()).slice(0, 120);
                    throw new Error(`HTTP ${res.status}: ${errText}`);
                }
                if (!ct.includes('application/json')) {
                    throw new Error('Non-JSON response (login expired?)');
                }
                const data = await res.json();
                if (!data.ok || !Array.isArray(data.students)) return;

                const seen = new Set();
                data.students.forEach((s) => {
                    const mid = Number(s.monitoring_id);
                    seen.add(mid);
                    const existing = this.students.get(mid) || {};
                    this.students.set(mid, {
                        ...existing,
                        ...s,
                        monitoring_id: mid,
                        is_online: existing.is_online ?? false
                    });
                    this._ensureTile(mid);
                    if (this.socket?.connected && mid) {
                        this.socket.emit('watch_monitoring', { monitoring_id: mid });
                    }
                });
                this.students.forEach((s, mid) => {
                    if (!seen.has(mid) && !s.terminated) {
                        s.is_online = false;
                        this._updateTileMeta(mid);
                    }
                });
                this._updateStats();
                this._loadRemovedStudents();
            } catch (err) {
                console.error('[LiveMonitor] bootstrap', err);
            }
        }

        async _loadRemovedStudents() {
            if (!this.dom.removedList) return;
            try {
                const testId = this.dom.filterTest?.value || '';
                const url = testId
                    ? `get_removed_students.php?test_id=${encodeURIComponent(testId)}`
                    : 'get_removed_students.php';
                const res = await fetch(url, { credentials: 'same-origin', headers: { Accept: 'application/json' } });
                const data = await res.json();
                if (!data.ok) return;
                this.dom.removedList.textContent = '';
                if (!data.students.length) {
                    const empty = document.createElement('div');
                    empty.className = 'empty-note';
                    empty.textContent = 'No students waiting for rejoin approval.';
                    this.dom.removedList.appendChild(empty);
                    return;
                }
                data.students.forEach((s) => {
                    const row = document.createElement('div');
                    row.className = 'removed-row';
                    row.dataset.mid = String(s.monitoring_id);
                    row.innerHTML = [
                        '<div><strong>', this._esc(s.student_name), '</strong>',
                        '<small>', this._esc(s.student_id), '</small>',
                        '<div class="mini">', this._esc(s.test_title), ' · ', this._esc(s.status), '</div></div>',
                        '<button type="button" class="btn btn-sm btn-warn btn-allow-rejoin">Allow rejoin</button>'
                    ].join('');
                    row.querySelector('.btn-allow-rejoin')?.addEventListener('click', (e) => {
                        e.preventDefault();
                        this._allowRejoin(Number(s.monitoring_id), row);
                    });
                    this.dom.removedList.appendChild(row);
                });
            } catch (_e) {}
        }

        _removeFromRemovedList(mid) {
            if (!this.dom.removedList || !mid) return;
            const row = this.dom.removedList.querySelector(`.removed-row[data-mid="${mid}"]`);
            if (row) row.remove();
            if (!this.dom.removedList.querySelector('.removed-row')) {
                const empty = document.createElement('div');
                empty.className = 'empty-note';
                empty.textContent = 'No students waiting for rejoin approval.';
                this.dom.removedList.appendChild(empty);
            }
        }

        _ensureTile(mid) {
            if (this.tiles.has(mid)) {
                this._updateTileMeta(mid);
                return;
            }
            const s = this.students.get(mid);
            if (!s) return;

            const card = document.createElement('article');
            card.className = `student-card ${RISK_CLASS[s.risk_level] || 'risk-normal'}`;
            card.dataset.mid = String(mid);
            card.id = `tile-${mid}`;

            const videoWrapEl = document.createElement('div');
            videoWrapEl.className = 'tile-video-wrap';

            const video = document.createElement('video');
            video.autoplay = true;
            video.playsInline = true;
            video.setAttribute('playsinline', 'true');
            video.setAttribute('webkit-playsinline', 'true');
            video.muted = true;
            video.className = 'tile-video';

            const overlay = document.createElement('div');
            overlay.className = 'tile-overlay';
            overlay.innerHTML = `
                <div class="tile-top">
                    <span class="live-pill" data-live-pill>Offline</span>
                    <span class="vio-badge" data-vio-badge>0</span>
                </div>
                <div class="tile-bottom">
                    <strong data-student-name></strong>
                    <small data-student-id></small>
                    <div class="tile-meta">
                        <span data-progress>Q: —</span>
                        <span data-risk-label></span>
                    </div>
                    <div class="tile-current-activity">
                        <small>Student Current Activity</small>
                        <span data-current-activity>—</span>
                    </div>
                </div>
            `;

            videoWrapEl.appendChild(video);
            videoWrapEl.appendChild(overlay);

            const actions = document.createElement('div');
            actions.className = 'tile-actions';
            actions.innerHTML = `
                <button type="button" data-act="logs" title="View logs"><i class="fas fa-list"></i></button>
                <button type="button" data-act="chat" title="Chat"><i class="fas fa-comment"></i><span class="chat-badge" hidden>0</span></button>
                <button type="button" data-act="warn" title="Warning"><i class="fas fa-exclamation"></i></button>
                <button type="button" data-act="terminate" title="Terminate"><i class="fas fa-ban"></i></button>
                <button type="button" data-act="rejoin" title="Allow rejoin" hidden><i class="fas fa-undo"></i></button>
            `;

            card.appendChild(videoWrapEl);
            card.appendChild(actions);

            actions.querySelectorAll('button').forEach((btn) => {
                btn.addEventListener('click', (e) => {
                    e.stopPropagation();
                    this._tileAction(mid, btn.dataset.act);
                });
            });

            card.addEventListener('click', () => this.selectStudent(mid));

            this.dom.grid?.appendChild(card);
            this.tiles.set(mid, card);
            this.videos.set(mid, video);
            this._updateTileMeta(mid);
        }

        _updateTileMeta(mid) {
            const s = this.students.get(mid);
            const card = this.tiles.get(mid);
            if (!s || !card) return;

            const risk = this._computeRisk(s);
            s.risk_level = risk.level;
            s.risk_label = risk.label;
            s.risk_score = risk.score;

            card.className = `student-card ${RISK_CLASS[risk.level] || 'risk-normal'}`;
            if (this._flashUntil.get(mid) > Date.now()) {
                card.classList.add('flash-alert');
            }

            const set = (sel, text) => {
                const el = card.querySelector(sel);
                if (el) el.textContent = text;
            };
            set('[data-student-name]', s.student_name || s.student_id);
            set('[data-student-id]', s.student_id);
            set('[data-vio-badge]', String(s.total_violations ?? 0));
            set('[data-progress]', `Q: ${s.answered_count ?? 0}${s.active_question ? ' · #' + s.active_question : ''}`);
            set('[data-current-activity]', s.latest_activity || '—');
            set('[data-risk-label]', risk.label);

            const pill = card.querySelector('[data-live-pill]');
            if (pill) {
                const online = !!s.is_online;
                pill.textContent = online ? 'Live' : 'Offline';
                pill.classList.toggle('on', online);
            }

            const rejoinBtn = card.querySelector('[data-act="rejoin"]');
            if (rejoinBtn) {
                rejoinBtn.hidden = !s.terminated;
            }
        }

        _computeRisk(s) {
            if (s.terminated) return { level: 'danger', label: 'Terminated', score: 100 };
            if (!s.is_online) return { level: 'gray', label: 'Disconnected', score: 0 };
            const v = Number(s.total_violations || 0);
            const tabAway = Number(s.tab_away_seconds || 0);
            let score = Math.min(100, v * 8 + (tabAway > 15 ? 15 : 0));
            if (v >= 5 || score >= 70) return { level: 'danger', label: 'High Risk', score };
            if (v >= 2 || score >= 30) return { level: 'warning', label: 'Warning', score };
            return { level: 'normal', label: 'Normal', score };
        }

        _attachVideo(mid, stream) {
            const video = this.videos.get(mid);
            if (!video || !stream) return;
            if (video.dataset.msid === stream.id && video.srcObject === stream) return;
            video.dataset.msid = stream.id;
            video.srcObject = stream;
            const playPromise = video.play();
            if (playPromise && typeof playPromise.catch === 'function') {
                playPromise.catch(() => {
                    video.muted = true;
                    video.play().catch(() => {});
                });
            }
            const s = this.students.get(mid);
            if (s) {
                s.is_online = true;
                this._updateTileMeta(mid);
            }
        }

        _setTileConnection(mid, connected) {
            const s = this.students.get(mid);
            if (s) {
                s.is_online = connected;
                this._updateTileMeta(mid);
            }
        }

        _handleStudentJoined(p) {
            this._registerStudentConnection(p, true);
        }

        _registerStudentConnection(p, logActivity) {
            const mid = Number(p.monitoring_id);
            if (!mid) return;
            const s = this.students.get(mid) || {};
            Object.assign(s, {
                monitoring_id: mid,
                student_id: p.student_id,
                test_id: p.test_id,
                socket_id: p.socket_id,
                is_online: true,
                latest_activity: 'Connected'
            });
            this.students.set(mid, s);
            this._ensureTile(mid);
            if (p.socket_id) {
                this.webrtc.registerStudentSocket(mid, p.socket_id);
                this.socket.emit('watch_monitoring', { monitoring_id: mid });
                const sid = p.socket_id;
                setTimeout(() => {
                    this.webrtc.negotiateMonitor(mid, sid, true);
                }, 500);
            }
            if (!logActivity) return;
            const now = Date.now();
            if (now - (this._joinActivityDedupe.get(mid) || 0) < 20000) return;
            this._joinActivityDedupe.set(mid, now);
            this._appendActivity({
                monitoring_id: mid,
                student_id: p.student_id,
                activity_type: 'CONNECTED',
                activity_value: 'Student online',
                timestamp: p.timestamp
            });
        }

        _handleExamEvent(ev) {
            if (!ev) return;
            /* STREAM_READY, violations, and answers are handled by dedicated events
               (student_joined, violation_alert, student_progress, live_activity). */
            if (ev.event_type === 'STREAM_READY') {
                const mid = Number(ev.monitoring_id);
                const sid = ev.payload?.socket_id;
                this._registerStudentConnection({
                    monitoring_id: mid,
                    student_id: ev.student_id,
                    test_id: ev.test_id,
                    socket_id: sid,
                    timestamp: ev.timestamp
                }, false);
                return;
            }
            if (['COPY', 'PASTE', 'CUT', 'AI_DETECTION', 'TAB_SWITCH', 'FULLSCREEN_EXIT', 'TAB_TIMEOUT', 'EXAM_TERMINATED'].includes(ev.event_type)) {
                return;
            }
            if (['ANSWER_UPDATED', 'ANSWER_TYPING', 'QUESTION_ANSWERED'].includes(ev.event_type)) {
                return;
            }
        }

        _handleViolation(p) {
            const mid = Number(p.monitoring_id);
            const type = p.violation_type || p.event_type || 'VIOLATION';
            const key = `${mid}:${type}`;
            const now = Date.now();
            if (now - (this._violationDedupe.get(key) || 0) < 6000) return;
            this._violationDedupe.set(key, now);

            const s = this.students.get(mid) || { monitoring_id: mid, student_id: p.student_id };
            s.total_violations = Number(s.total_violations || 0) + 1;
            s.latest_activity = VIOLATION_LABELS[type] || type;
            this.students.set(mid, s);
            this._ensureTile(mid);
            this._updateTileMeta(mid);
            this._flashUntil.set(mid, now + 8000);
            this._updateTileMeta(mid);

            const label = VIOLATION_LABELS[type] || type;
            const msg = `${s.student_id || p.student_id}: ${label}`;
            this._showAlertPopup(msg, p.risk_level === 'danger' ? 'danger' : 'warning');
            if (!['ANSWER_UPDATED', 'ANSWER_TYPING', 'QUESTION_ANSWERED'].includes(type)) {
                this._appendActivity({
                    monitoring_id: mid,
                    student_id: p.student_id,
                    activity_type: type,
                    activity_value: label,
                    timestamp: p.timestamp || new Date().toISOString()
                });
            }

            if (['danger', 'critical', 'high'].includes(String(p.risk_level)) && window.ProctorSound) {
                ProctorSound.enqueue('violation');
            }
        }

        _handleProgress(p) {
            const mid = Number(p.monitoring_id);
            const s = this.students.get(mid) || {
                monitoring_id: mid,
                student_id: p.student_id
            };
            if (p.active_question != null) s.active_question = p.active_question;
            const label = this._formatAnswerActivity({
                event_type: p.event_type,
                active_question: p.active_question,
                payload: p.payload || {}
            });
            s.latest_activity = label;
            if (p.event_type === 'QUESTION_ANSWERED' || p.event_type === 'ANSWER_UPDATED') {
                s.answered_count = Number(s.answered_count || 0) + 1;
            }
            this.students.set(mid, s);
            this._ensureTile(mid);
            this._updateTileMeta(mid);
            if (['ANSWER_UPDATED', 'ANSWER_TYPING', 'QUESTION_ANSWERED'].includes(p.event_type)) {
                this._appendActivity({
                    monitoring_id: mid,
                    student_id: p.student_id,
                    activity_type: p.event_type,
                    activity_value: label,
                    details: p.payload,
                    timestamp: p.timestamp
                });
            }
        }

        _enrichActivityPayload(p) {
            if (!p) return p;
            const copy = { ...p };
            const type = copy.activity_type || '';
            if (['ANSWER_UPDATED', 'ANSWER_TYPING', 'QUESTION_ANSWERED'].includes(type)) {
                copy.activity_value = this._formatAnswerActivity({
                    event_type: type,
                    active_question: copy.activity_value,
                    payload: copy.details || copy.payload || {}
                });
            } else if (!copy.activity_value) {
                copy.activity_value = ACTIVITY_LABELS[type] || VIOLATION_LABELS[type] || type;
            }
            return copy;
        }

        _activityKey(p) {
            const mid = Number(p.monitoring_id) || 0;
            const type = String(p.activity_type || '');
            const value = String(p.activity_value || '').slice(0, 96);
            const ts = p.timestamp ? Math.floor(new Date(p.timestamp).getTime() / 1000) : Math.floor(Date.now() / 1000);
            return `${mid}|${type}|${value}|${ts}`;
        }

        _formatAnswerActivity(p) {
            const type = p.event_type || p.activity_type || '';
            const payload = p.payload || p.details || {};
            const qText = payload.question_text || '';
            const selected = payload.selected_label || payload.option_label || '';
            const preview = payload.answer_preview || payload.answer_value || '';
            if (qText) {
                if (selected) {
                    return `Question: ${qText} · Selected Answer: ${selected}`;
                }
                if (type === 'ANSWER_TYPING') {
                    const typing = preview ? ` · Typing: ${String(preview).slice(0, 60)}` : '';
                    return `Question: ${qText}${typing}`;
                }
                if (preview) {
                    return `Question: ${qText} · Answer: ${String(preview).slice(0, 80)}`;
                }
                return `Question: ${qText}`;
            }
            const q = p.active_question ?? payload.question_key ?? '';
            const qLabel = q !== '' && q != null ? `Q${q}` : 'question';
            if (type === 'ANSWER_TYPING') {
                return preview ? `Typing ${qLabel}: ${String(preview).slice(0, 60)}` : `Typing ${qLabel}`;
            }
            if (type === 'QUESTION_ANSWERED' || type === 'ANSWER_UPDATED') {
                if (selected) return `${qLabel}: Selected ${selected}`;
                return preview ? `Answered ${qLabel}: ${String(preview).slice(0, 48)}` : `Answered ${qLabel}`;
            }
            return ACTIVITY_LABELS[type] || type || 'Activity';
        }

        _handleDisconnected(p) {
            const mid = Number(p.monitoring_id);
            const s = this.students.get(mid);
            if (s) {
                s.is_online = false;
                s.latest_activity = 'Disconnected';
                this._updateTileMeta(mid);
            }
            this.webrtc.disposePeer(mid);
            this._appendActivity({
                monitoring_id: mid,
                student_id: p.student_id,
                activity_type: 'DISCONNECTED',
                activity_value: 'Socket disconnected',
                timestamp: p.timestamp
            });
        }

        _handleTerminated(p) {
            const mid = Number(p.monitoring_id);
            const s = this.students.get(mid) || {};
            s.terminated = true;
            s.is_online = false;
            s.latest_activity = 'Terminated';
            s.risk_level = 'danger';
            this.students.set(mid, s);
            this._updateTileMeta(mid);
            this.webrtc.disposeAllForStudent(mid);
            this._showAlertPopup(`${p.student_id} — exam terminated`, 'danger');
            this.bootstrapStudents();
        }

        _appendActivity(p) {
            if (!this.dom.feed || !p) return;
            const key = this._activityKey(p);
            const now = Date.now();
            if (this._activityDedupe.has(key)) return;
            this._activityDedupe.set(key, now);
            if (this._activityDedupe.size > 400) {
                const cutoff = now - 120000;
                this._activityDedupe.forEach((t, k) => {
                    if (t < cutoff) this._activityDedupe.delete(k);
                });
            }
            const row = document.createElement('div');
            row.className = 'feed-item';
            if (['ANSWER_UPDATED', 'QUESTION_ANSWERED', 'ANSWER_TYPING'].includes(p.activity_type)) {
                row.classList.add('feed-answer');
            }
            const ts = p.timestamp ? new Date(p.timestamp).toLocaleTimeString() : new Date().toLocaleTimeString();
            const who = p.student_id ? this._esc(p.student_id) : 'All students';
            const text = this._esc(p.activity_value || ACTIVITY_LABELS[p.activity_type] || p.activity_type || '');
            row.innerHTML = `<span class="feed-time">${ts}</span><strong>${who}</strong><span>${text}</span>`;
            this.dom.feed.prepend(row);
            while (this.dom.feed.children.length > 120) {
                this.dom.feed.lastChild.remove();
            }
        }

        _showAlertPopup(message, level) {
            if (!this.dom.alertPopupHost) return;
            const popup = document.createElement('div');
            popup.className = `alert-popup alert-${level}`;
            popup.textContent = message;
            this.dom.alertPopupHost.appendChild(popup);
            setTimeout(() => popup.remove(), 6000);
            if (this.dom.alerts) {
                this.dom.alerts.textContent = message;
                this.dom.alerts.className = 'live-alert show ' + (level === 'danger' ? '' : 'warning');
                setTimeout(() => {
                    this.dom.alerts.classList.remove('show');
                }, 5000);
            }
        }

        _esc(v) {
            return String(v ?? '').replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
        }

        _updateStats() {
            let online = 0;
            let alerts = 0;
            this.students.forEach((s) => {
                if (s.is_online) online += 1;
                alerts += Number(s.total_violations || 0);
            });
            if (this.dom.onlineCount) this.dom.onlineCount.textContent = String(online);
            if (this.dom.alertCount) this.dom.alertCount.textContent = String(alerts);
        }

        _healthCheck() {
            if (!this.socket?.connected) return;
            this.webrtc.purgeDeadPeers();
            this.students.forEach((s, mid) => {
                if (s.is_online && s.socket_id) {
                    const pc = this.webrtc.getMonitorPc(mid);
                    if (!pc || ['failed', 'closed'].includes(pc.connectionState)) {
                        this.webrtc.negotiateMonitor(mid, s.socket_id, false);
                    }
                }
            });
        }

        async selectStudent(mid) {
            this.selectedMid = mid;
            this.tiles.forEach((card, id) => card.classList.toggle('selected', id === mid));
            const unread = this.unreadChat.get(mid) || 0;
            if (unread) {
                this.unreadChat.set(mid, 0);
                const badge = this.tiles.get(mid)?.querySelector('.chat-badge');
                if (badge) {
                    badge.hidden = true;
                    badge.textContent = '0';
                }
            }
        }

        async _tileAction(mid, action) {
            const s = this.students.get(mid);
            if (!s) return;
            this.selectStudent(mid);

            if (action === 'logs') await this._openLogs(mid);
            if (action === 'chat') await this._openChat(mid);
            if (action === 'warn') await this._sendWarning(mid);
            if (action === 'terminate') await this._terminate(mid);
            if (action === 'rejoin') await this._allowRejoin(mid);
        }

        async loadAttendance() {
            if (!this.dom.attendanceCompleted || !this.dom.attendanceNotCompleted) return;
            this._selectDefaultTestFilter();
            const testId = this.dom.filterTest?.value || '';
            if (!testId) {
                this.dom.attendanceCompleted.innerHTML = '<tr><td colspan="4" class="att-empty">No tests available for attendance.</td></tr>';
                this.dom.attendanceNotCompleted.innerHTML = '<tr><td colspan="4" class="att-empty">—</td></tr>';
                if (this.dom.attCompletedCount) this.dom.attCompletedCount.textContent = '0';
                if (this.dom.attNotCompletedCount) this.dom.attNotCompletedCount.textContent = '0';
                return;
            }
            try {
                const res = await fetch(`get_test_attendance.php?test_id=${encodeURIComponent(testId)}`, {
                    credentials: 'same-origin',
                    headers: { Accept: 'application/json' }
                });
                const data = await res.json();
                if (!data.ok) {
                    this.dom.attendanceCompleted.innerHTML = `<tr><td colspan="4" class="att-empty">${this._esc(data.msg || 'Unable to load')}</td></tr>`;
                    return;
                }
                const renderRow = (s) => `<tr>
                        <td>${this._esc(s.student_name)}</td>
                        <td>${this._esc(s.student_id)}</td>
                        <td>${this._esc(s.section)}</td>
                        <td>${this._esc(s.current_status)}</td>
                    </tr>`;
                const doneRows = (data.completed || []).map(renderRow);
                const pendingRows = (data.not_completed || []).map(renderRow);
                this.dom.attendanceCompleted.innerHTML = doneRows.length
                    ? doneRows.join('')
                    : '<tr><td colspan="4" class="att-empty">No students have submitted this test yet.</td></tr>';
                this.dom.attendanceNotCompleted.innerHTML = pendingRows.length
                    ? pendingRows.join('')
                    : '<tr><td colspan="4" class="att-empty">All enrolled students have completed this test.</td></tr>';
                if (this.dom.attCompletedCount) this.dom.attCompletedCount.textContent = String(data.counts?.completed ?? doneRows.length);
                if (this.dom.attNotCompletedCount) this.dom.attNotCompletedCount.textContent = String(data.counts?.not_completed ?? pendingRows.length);
            } catch (err) {
                this.dom.attendanceCompleted.innerHTML = `<tr><td colspan="4" class="att-empty">${this._esc(err.message)}</td></tr>`;
            }
        }

        async _openLogs(mid) {
            if (!this.dom.logsModal || !this.dom.logsBody) return;
            this.dom.logsBody.textContent = 'Loading…';
            const modal = bootstrap.Modal.getOrCreateInstance(this.dom.logsModal);
            modal.show();
            try {
                const res = await fetch(`get_student_activity.php?monitoring_id=${encodeURIComponent(mid)}`);
                if (!res.ok) throw new Error(`HTTP ${res.status}`);
                const data = await res.json();
                if (!data.ok) {
                    this.dom.logsBody.textContent = data.msg || 'Could not load logs';
                    return;
                }
                const parts = [];
                parts.push(`<h6>${this._esc(data.session?.student_name)}</h6>`);
                parts.push(`<p class="mini">Test: ${this._esc(data.session?.test_title)} · Status: ${this._esc(data.session?.status)}</p>`);
                if (data.summary) {
                    parts.push(`<p class="mini">Violations: ${data.summary.total_violations ?? 0} · High risk: ${data.summary.high_risk ?? 0} · Tab switches: ${data.summary.tab_switches ?? 0}</p>`);
                }
                if (data.misconduct_log?.length) {
                    parts.push('<h6 class="log-section-title">Student misconduct log</h6><ul class="log-misconduct">');
                    data.misconduct_log.forEach((m) => {
                        const sev = m.severity ? `<span class="log-sev">${this._esc(m.severity)}</span>` : '';
                        parts.push(`<li>[${this._esc(m.created_at)}] <strong>${this._esc(m.event_type)}</strong> ${sev}<br>${this._esc(m.description)}</li>`);
                    });
                    parts.push('</ul>');
                } else {
                    parts.push('<p class="mini">No misconduct events recorded.</p>');
                }
                if (data.timeline_log?.length) {
                    parts.push('<h6 class="log-section-title">Event timeline</h6><ul class="log-timeline">');
                    data.timeline_log.forEach((e) => {
                        parts.push(`<li>[${this._esc(e.created_at)}] <strong>${this._esc(e.event_type)}</strong> — ${this._esc(e.description)}</li>`);
                    });
                    parts.push('</ul>');
                }
                this.dom.logsBody.innerHTML = parts.join('');
            } catch (err) {
                this.dom.logsBody.textContent = `Error: ${err.message}`;
            }
        }

        async _openChat(mid) {
            if (!this.dom.chatModal) return;
            this.dom.chatModal.dataset.mid = String(mid);
            const modal = bootstrap.Modal.getOrCreateInstance(this.dom.chatModal);
            modal.show();
            await this._loadChat(mid);
            this.socket?.emit('watch_monitoring', { monitoring_id: mid });
        }

        async _loadChat(mid) {
            if (!this.dom.chatMessages) return;
            try {
                const res = await fetch(`get_chat_messages.php?monitoring_id=${encodeURIComponent(mid)}`);
                const data = await res.json();
                this.dom.chatMessages.textContent = '';
                this._chatLineKeys.clear();
                if (!data.ok || !data.messages?.length) {
                    this.dom.chatMessages.innerHTML = '<div class="empty-note">No messages yet.</div>';
                    return;
                }
                data.messages.forEach((m) => this._renderChatLine(m));
                this.dom.chatMessages.scrollTop = this.dom.chatMessages.scrollHeight;
            } catch (err) {
                this.dom.chatMessages.textContent = err.message;
            }
        }

        _chatLineKey(m) {
            const mid = Number(m.monitoring_id) || 0;
            const role = String(m.sender_role || '');
            const body = String(m.message_body ?? m.message ?? '').trim();
            const ts = m.created_at ? Math.floor(new Date(m.created_at).getTime() / 1000) : Math.floor(Date.now() / 1000);
            return `${mid}|${role}|${body}|${ts}`;
        }

        _renderChatLine(m) {
            if (!this.dom.chatMessages) return;
            const body = String(m.message_body ?? m.message ?? '').trim();
            if (!body) return;
            const key = this._chatLineKey({ ...m, message_body: body });
            if (this._chatLineKeys.has(key)) return;
            this._chatLineKeys.add(key);
            if (this._chatLineKeys.size > 200) {
                const first = this._chatLineKeys.values().next().value;
                if (first) this._chatLineKeys.delete(first);
            }
            const empty = this.dom.chatMessages.querySelector('.empty-note');
            if (empty) empty.remove();
            const line = document.createElement('div');
            line.className = `chat-line ${m.sender_role === 'lecturer' ? 'out' : 'in'}`;
            const ts = m.created_at ? new Date(m.created_at).toLocaleTimeString() : '';
            const prefix = m.event_type === 'CHAT_BROADCAST' ? '[All] ' : '';
            line.innerHTML = `<span class="chat-ts">${ts}</span><span class="chat-body">${prefix}${this._esc(body)}</span>`;
            this.dom.chatMessages.appendChild(line);
        }

        _handleIncomingChat(p) {
            const mid = Number(p.monitoring_id);
            if (!mid) return;
            if (p.sender_role === 'lecturer') {
                if (this.dom.chatModal?.dataset.mid === String(mid)) {
                    this._renderChatLine({
                        sender_role: 'lecturer',
                        message_body: p.message,
                        created_at: p.created_at,
                        monitoring_id: mid
                    });
                }
                return;
            }
            if (p.sender_role === 'student') {
                const count = (this.unreadChat.get(mid) || 0) + 1;
                this.unreadChat.set(mid, count);
                const badge = this.tiles.get(mid)?.querySelector('.chat-badge');
                if (badge) {
                    badge.hidden = false;
                    badge.textContent = String(count);
                }
            }
            if (this.dom.chatModal?.dataset.mid === String(mid)) {
                this._renderChatLine({
                    sender_role: p.sender_role,
                    message_body: p.message,
                    created_at: p.created_at,
                    event_type: p.event_type,
                    monitoring_id: mid
                });
            } else {
                this._appendActivity({
                    monitoring_id: mid,
                    student_id: p.student_id,
                    activity_type: 'CHAT_STUDENT',
                    activity_value: `Message: ${String(p.message || '').slice(0, 80)}`,
                    timestamp: p.created_at
                });
            }
        }

        _sendChat() {
            const mid = Number(this.dom.chatModal?.dataset.mid);
            const text = this.dom.chatInput?.value?.trim();
            if (!mid || !text || !this.socket) return;
            const messageId = `lec-${mid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
            this.socket.emit('private_message', { monitoring_id: mid, message: text, message_id: messageId });
            this._renderChatLine({
                sender_role: 'lecturer',
                message_body: text,
                created_at: new Date().toISOString(),
                monitoring_id: mid
            });
            if (this.dom.chatInput) this.dom.chatInput.value = '';
        }

        _sendBroadcast() {
            const text = this.dom.broadcastInput?.value?.trim();
            if (!text || !this.socket) return;
            const testId = this.dom.filterTest?.value ? Number(this.dom.filterTest.value) : 0;
            this.socket.emit('broadcast_message', { test_id: testId, message: text });
            if (this.dom.broadcastInput) this.dom.broadcastInput.value = '';
            if (window.ProctorToast) {
                ProctorToast.show({
                    title: 'Broadcast sent',
                    message: testId > 0 ? 'Message sent to all students in this test.' : 'Message sent to all online students.',
                    level: 'success'
                });
            }
        }

        _sendWarning(mid) {
            const msg = window.prompt('Warning message to student:', 'Please focus on your exam and follow proctoring rules.');
            if (!msg || !this.socket) return;
            this.socket.emit('send_warning', { monitoring_id: mid, message: msg });
            if (window.ProctorToast) ProctorToast.show({ title: 'Warning sent', message: msg, level: 'warning' });
        }

        async _terminate(mid) {
            if (!window.confirm('Terminate this student exam session?')) return;
            this.socket?.emit('terminate_student', { monitoring_id: mid, reason: 'LECTURER_TERMINATED' });
            const fd = new FormData();
            fd.append('monitoring_id', String(mid));
            fd.append('return', 'lecturer_live_monitor');
            await fetch('lecturer_terminate_monitoring.php', { method: 'POST', body: fd }).catch(() => {});
        }

        async _allowRejoin(mid, rowEl = null) {
            if (!mid) return;
            if (!window.confirm('Allow this student to rejoin the exam?')) return;
            const btn = rowEl?.querySelector('.btn-allow-rejoin');
            if (btn) {
                btn.disabled = true;
                btn.textContent = 'Approving…';
            }
            const fd = new FormData();
            fd.append('monitoring_id', String(mid));
            fd.append('ajax', '1');
            try {
                const res = await fetch('approve_student_rejoin.php', {
                    method: 'POST',
                    body: fd,
                    credentials: 'same-origin',
                    headers: { 'X-Requested-With': 'XMLHttpRequest' }
                });
                const data = await res.json();
                if (!data.ok) {
                    if (btn) {
                        btn.disabled = false;
                        btn.textContent = 'Allow rejoin';
                    }
                    if (window.ProctorToast) {
                        ProctorToast.show({ title: 'Rejoin failed', message: data.message || 'Could not restore session', level: 'danger' });
                    }
                    return;
                }
                this._removeFromRemovedList(mid);
                this.socket?.emit('allow_rejoin', { monitoring_id: mid });
                const s = this.students.get(mid);
                if (s) {
                    s.terminated = false;
                    s.status = 'running';
                    this._updateTileMeta(mid);
                }
                await this._loadRemovedStudents();
                await this.bootstrapStudents();
                if (window.ProctorToast) {
                    ProctorToast.show({ title: 'Rejoin approved', message: 'Student can resume the exam now.', level: 'success' });
                }
            } catch (err) {
                if (btn) {
                    btn.disabled = false;
                    btn.textContent = 'Allow rejoin';
                }
                if (window.ProctorToast) {
                    ProctorToast.show({ title: 'Rejoin error', message: err.message, level: 'danger' });
                }
            }
        }
    }

    return { App };
})();
