/**
 * Short notification tone when lecturer sends a chat message (exam page).
 */
window.StudentChatAlert = (function () {
    let ctx = null;
    let unlocked = false;

    function unlock() {
        try {
            if (!ctx) {
                ctx = new (window.AudioContext || window.webkitAudioContext)();
            }
            if (ctx.state === 'suspended') {
                ctx.resume();
            }
            unlocked = true;
        } catch (_e) {}
    }

    async function play() {
        try {
            unlock();
            if (!ctx || !unlocked) return;
            if (ctx.state === 'suspended') {
                await ctx.resume();
            }
            const osc = ctx.createOscillator();
            const gain = ctx.createGain();
            osc.type = 'sine';
            osc.frequency.setValueAtTime(880, ctx.currentTime);
            osc.frequency.setValueAtTime(1175, ctx.currentTime + 0.08);
            gain.gain.setValueAtTime(0.0001, ctx.currentTime);
            gain.gain.exponentialRampToValueAtTime(0.12, ctx.currentTime + 0.02);
            gain.gain.exponentialRampToValueAtTime(0.0001, ctx.currentTime + 0.35);
            osc.connect(gain);
            gain.connect(ctx.destination);
            osc.start();
            osc.stop(ctx.currentTime + 0.36);
        } catch (_e) {}
    }

    window.addEventListener('pointerdown', unlock, { once: true, passive: true });
    window.addEventListener('keydown', unlock, { once: true, passive: true });

    return { play, unlock };
})();
