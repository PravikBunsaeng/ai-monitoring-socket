'use strict';

const buckets = new Map();

function allow(key, max, windowMs) {
  const now = Date.now();
  let entry = buckets.get(key);
  if (!entry || now - entry.start > windowMs) {
    entry = { start: now, count: 0 };
    buckets.set(key, entry);
  }
  entry.count += 1;
  if (entry.count > max) {
    return false;
  }
  return true;
}

setInterval(() => {
  const now = Date.now();
  for (const [key, entry] of buckets.entries()) {
    if (now - entry.start > 120000) {
      buckets.delete(key);
    }
  }
}, 60000);

module.exports = { allow };
