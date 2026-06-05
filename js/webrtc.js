/**
 * Lecturer-side WebRTC — one monitor peer per student (video receive only).
 */
window.LecturerWebRTC = (function () {
    const RTC_CONFIG = {
        iceServers: [
            { urls: 'stun:stun.l.google.com:19302' },
            { urls: 'stun:stun1.l.google.com:19302' },
            { urls: 'stun:stun2.l.google.com:19302' }
        ],
        iceCandidatePoolSize: 10
    };

    const NEGOTIATE_COOLDOWN_MS = 2000;

    class Manager {
        constructor({ onRemoteStream, onConnectionState, onError }) {
            this.onRemoteStream = onRemoteStream || (() => {});
            this.onConnectionState = onConnectionState || (() => {});
            this.onError = onError || (() => {});
            this.monitorPeers = new Map();
            this.pendingIce = new Map();
            this.lastNegotiateAt = new Map();
            this.studentSocketByMid = new Map();
            this.attachedMsid = new Map();
            this._socket = null;
            this._lecturerSocketId = '';
        }

        setSocket(socket, lecturerSocketId) {
            this._socket = socket;
            this._lecturerSocketId = lecturerSocketId || socket?.id || '';
        }

        peerKey(mid) {
            return `monitor:${mid}`;
        }

        registerStudentSocket(monitoringId, studentSocketId) {
            if (!monitoringId || !studentSocketId) return;
            this.studentSocketByMid.set(Number(monitoringId), String(studentSocketId));
        }

        emitSignal(toSocketId, monitoringId, data) {
            if (!this._socket || !toSocketId) return;
            this._socket.emit('webrtc_signal', {
                to_socket_id: toSocketId,
                monitoring_id: monitoringId,
                channel: 'monitor',
                ...data
            });
        }

        async flushPendingIce(key, pc) {
            const queue = this.pendingIce.get(key) || [];
            if (!queue.length) return;
            for (const c of queue) {
                try {
                    await pc.addIceCandidate(new RTCIceCandidate(c));
                } catch (_e) {}
            }
            this.pendingIce.delete(key);
        }

        disposePeer(mid) {
            const pc = this.monitorPeers.get(mid);
            if (pc) {
                try {
                    pc.getSenders?.().forEach((s) => {
                        try { pc.removeTrack(s); } catch (_e) {}
                    });
                    pc.close();
                } catch (_e) {}
                this.monitorPeers.delete(mid);
            }
            this.pendingIce.delete(this.peerKey(mid));
            this.attachedMsid.delete(mid);
        }

        disposeAllForStudent(mid) {
            this.disposePeer(mid);
        }

        getMonitorPc(mid) {
            return this.monitorPeers.get(mid) || null;
        }

        async negotiateMonitor(monitoringId, studentSocketId, force) {
            const mid = Number(monitoringId);
            const studentSid = String(studentSocketId || this.studentSocketByMid.get(mid) || '');
            if (!mid || !studentSid || !this._socket) return null;

            this.registerStudentSocket(mid, studentSid);

            const now = Date.now();
            const last = this.lastNegotiateAt.get(mid) || 0;
            if (!force && now - last < NEGOTIATE_COOLDOWN_MS) {
                return this.monitorPeers.get(mid) || null;
            }

            const existing = this.monitorPeers.get(mid);
            if (existing && existing.connectionState === 'connected') {
                return existing;
            }
            if (existing) {
                this.disposePeer(mid);
            }

            this.lastNegotiateAt.set(mid, now);

            const pc = new RTCPeerConnection(RTC_CONFIG);
            this.monitorPeers.set(mid, pc);

            pc.onicecandidate = ({ candidate }) => {
                if (candidate) this.emitSignal(studentSid, mid, { candidate });
            };

            pc.ontrack = (evt) => {
                let stream = evt.streams && evt.streams[0];
                if (!stream && evt.track) {
                    stream = new MediaStream([evt.track]);
                }
                if (!stream) return;
                if (this.attachedMsid.get(mid) === stream.id) return;
                this.attachedMsid.set(mid, stream.id);
                this.onRemoteStream(mid, stream, evt.track);
            };

            pc.onconnectionstatechange = () => {
                const st = pc.connectionState;
                this.onConnectionState(mid, st);
                if (st === 'failed') {
                    console.warn('[WebRTC] monitor failed', mid);
                    this.disposePeer(mid);
                }
            };

            try {
                pc.addTransceiver('video', { direction: 'recvonly' });
                const offer = await pc.createOffer();
                await pc.setLocalDescription(offer);
                this.emitSignal(studentSid, mid, { sdp: pc.localDescription });
            } catch (err) {
                this.onError({ title: 'WebRTC Error', message: err.message, level: 'danger' });
                this.disposePeer(mid);
                return null;
            }

            return pc;
        }

        async handleSignal(signal) {
            if (!signal || signal.channel !== 'monitor') return;
            const mid = Number(signal.monitoring_id);
            const fromSid = String(signal.from_socket_id || '');
            if (fromSid) this.registerStudentSocket(mid, fromSid);

            let pc = this.monitorPeers.get(mid);

            if (!pc || ['closed', 'failed'].includes(pc.connectionState)) {
                if (signal.sdp?.type === 'answer') {
                    pc = new RTCPeerConnection(RTC_CONFIG);
                    this.monitorPeers.set(mid, pc);
                    this._bindMonitorHandlers(pc, mid, fromSid);
                } else {
                    return;
                }
            }

            const iceKey = this.peerKey(mid);

            if (signal.sdp) {
                const sdpType = signal.sdp.type;
                const state = pc.signalingState;
                if (sdpType === 'answer' && state !== 'have-local-offer') {
                    console.warn('[WebRTC] ignore answer in state', state, mid);
                    return;
                }
                if (sdpType === 'offer' && state === 'have-local-offer') {
                    return;
                }
                try {
                    await pc.setRemoteDescription(new RTCSessionDescription(signal.sdp));
                    await this.flushPendingIce(iceKey, pc);
                    if (sdpType === 'offer') {
                        const answer = await pc.createAnswer();
                        await pc.setLocalDescription(answer);
                        this.emitSignal(fromSid, mid, { sdp: pc.localDescription });
                    }
                } catch (err) {
                    console.error('[WebRTC] setRemoteDescription', err);
                    this.onError({ title: 'WebRTC SDP Error', message: err.message, level: 'danger' });
                }
            }

            if (signal.candidate) {
                if (pc.remoteDescription) {
                    try {
                        await pc.addIceCandidate(new RTCIceCandidate(signal.candidate));
                    } catch (err) {
                        console.warn('[WebRTC] ICE error', err.message);
                    }
                } else {
                    const q = this.pendingIce.get(iceKey) || [];
                    q.push(signal.candidate);
                    this.pendingIce.set(iceKey, q);
                }
            }
        }

        _bindMonitorHandlers(pc, mid, studentSid) {
            pc.onicecandidate = ({ candidate }) => {
                if (candidate) this.emitSignal(studentSid, mid, { candidate });
            };
            pc.ontrack = (evt) => {
                let stream = evt.streams && evt.streams[0];
                if (!stream && evt.track) {
                    stream = new MediaStream([evt.track]);
                }
                if (!stream) return;
                if (this.attachedMsid.get(mid) === stream.id) return;
                this.attachedMsid.set(mid, stream.id);
                this.onRemoteStream(mid, stream, evt.track);
            };
            pc.onconnectionstatechange = () => {
                this.onConnectionState(mid, pc.connectionState);
                if (pc.connectionState === 'failed') this.disposePeer(mid);
            };
        }

        purgeDeadPeers() {
            this.monitorPeers.forEach((pc, mid) => {
                if (['failed', 'disconnected', 'closed'].includes(pc.connectionState)) {
                    this.disposePeer(mid);
                }
            });
            const now = Date.now();
            this.lastNegotiateAt.forEach((ts, mid) => {
                if (now - ts > 60000) this.lastNegotiateAt.delete(mid);
            });
        }
    }

    return { Manager, RTC_CONFIG };
})();
