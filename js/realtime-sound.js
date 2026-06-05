window.ProctorSound = (() => {
    const queue = [];
    let isPlaying = false;
    let lastPlayedAt = 0;
    const cooldownMs = 1200;
    let sharedCtx = null;
    let unlocked = false;

    function enqueue(type = "violation") {
        queue.push(type);
        processQueue();
    }

    function unlock() {
        try {
            if (!sharedCtx) {
                sharedCtx = new (window.AudioContext || window.webkitAudioContext)();
            }
            if (sharedCtx.state === "suspended") {
                sharedCtx.resume();
            }
            unlocked = true;
        } catch (_err) {}
    }

    function processQueue() {
        if (isPlaying || queue.length === 0) return;
        if (!unlocked) return;
        const now = Date.now();
        if (now - lastPlayedAt < cooldownMs) {
            setTimeout(processQueue, cooldownMs - (now - lastPlayedAt));
            return;
        }
        isPlaying = true;
        lastPlayedAt = now;
        queue.shift();
        playTone().finally(() => {
            isPlaying = false;
            setTimeout(processQueue, 20);
        });
    }

    async function playTone() {
        try {
            if (!sharedCtx) {
                sharedCtx = new (window.AudioContext || window.webkitAudioContext)();
            }
            if (sharedCtx.state === "suspended") {
                await sharedCtx.resume();
            }
            const o1 = sharedCtx.createOscillator();
            const o2 = sharedCtx.createOscillator();
            const gain = sharedCtx.createGain();
            o1.type = "sine";
            o2.type = "triangle";
            o1.frequency.setValueAtTime(740, sharedCtx.currentTime);
            o2.frequency.setValueAtTime(880, sharedCtx.currentTime);
            gain.gain.setValueAtTime(0.001, sharedCtx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.08, sharedCtx.currentTime + 0.02);
            gain.gain.exponentialRampToValueAtTime(0.001, sharedCtx.currentTime + 0.38);
            o1.connect(gain);
            o2.connect(gain);
            gain.connect(sharedCtx.destination);
            o1.start();
            o2.start();
            o1.stop(sharedCtx.currentTime + 0.4);
            o2.stop(sharedCtx.currentTime + 0.4);
            await new Promise((resolve) => setTimeout(resolve, 420));
        } catch (_err) {}
    }

    const audioCtx = {
        get current() {
            return sharedCtx;
        }
    };

    window.addEventListener("pointerdown", unlock, { once: true, passive: true });
    window.addEventListener("keydown", unlock, { once: true, passive: true });

    return { enqueue, unlock, audioCtx };
})();

