import assert from 'node:assert/strict';
import { appendFileSync, mkdirSync, mkdtempSync, readSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';

import {
  createTranscriptObserver,
  observeTranscript,
  resolveTranscriptPath,
  resolvePendingDelivery,
} from '../scripts/daemon/channels/claude-code-queue-io.mjs';
import { createClaudeCodeQueueChannel } from '../scripts/daemon/channels/claude-code-queue.mjs';
import { ensureClaudeCodeChannelSchema } from '../scripts/daemon/channels/claude-code-queue-store.mjs';
import { openInstallDb } from '../scripts/daemon/db.mjs';

function temporaryDirectory(t) {
  const dir = mkdtempSync(path.join(os.tmpdir(), 'agmsg-cc-queue-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  return dir;
}

// Load-bearing property: a `queue-operation`/`enqueue` line fires on mere
// receipt, Held or not, so it must NEVER by itself cause a seat to be marked
// delivered -- never notify on evidence that could just as well be a hold.
// Only the `type:"user"` line whose `origin.body` carries the nonce, or the
// mid-turn `queued_command` attachment whose `prompt` carries it, counts.
// This reproduces both line shapes measured against a live Claude Code
// transcript in one file and asserts the queue-operation line alone is not
// enough, while the user/origin.body line is.
test('observeTranscript: a queue-operation enqueue line is not delivery evidence; the user/origin.body line and a mid-turn queued_command attachment are', (t) => {
  const dir = temporaryDirectory(t);
  const transcriptPath = path.join(dir, 'session.jsonl');
  const nonce = 'abc123-test-nonce';
  const marker = `[agmsg:${nonce}] New messages are waiting. Check your agmsg inbox.`;

  // Only the enqueue line present (held, or not yet acted on) — must read as
  // absent, not delivered.
  writeFileSync(transcriptPath, `${JSON.stringify({
    type: 'queue-operation',
    operation: 'enqueue',
    content: `<cross-session-message from="agmsgd">${marker}</cross-session-message>`,
  })}\n`);
  let observation = observeTranscript(transcriptPath, nonce);
  assert.equal(observation.state, 'absent');
  let resolved = resolvePendingDelivery({ observation });
  assert.equal(resolved.state, 'pending');

  // The real delivered record appended afterward (as the genuine sequence
  // measured: enqueue first, then the delivered user line) — now it must
  // read as delivered/confirmed.
  writeFileSync(transcriptPath, `${JSON.stringify({
    type: 'queue-operation',
    operation: 'enqueue',
    content: `<cross-session-message from="agmsgd">${marker}</cross-session-message>`,
  })}\n${JSON.stringify({
    type: 'user',
    isMeta: true,
    message: { role: 'user', content: 'Another Claude session sent a message...' },
    origin: { kind: 'peer', from: 'agmsgd', verifiedPeerPid: 12345, name: 'agmsgd', fromMode: 'bypass', body: marker },
  })}\n`);
  observation = observeTranscript(transcriptPath, nonce);
  assert.equal(observation.state, 'delivered');
  resolved = resolvePendingDelivery({ observation });
  assert.equal(resolved.state, 'confirmed');

  // Absorbed mid-turn: enqueue, remove, then a queued_command attachment and
  // no user line at all. The model read it, so it is delivered.
  writeFileSync(transcriptPath, `${JSON.stringify({
    type: 'queue-operation',
    operation: 'enqueue',
    content: `<cross-session-message from="agmsgd">${marker}</cross-session-message>`,
  })}\n${JSON.stringify({
    type: 'queue-operation',
    operation: 'remove',
    reason: 'absorbed_mid_turn',
    content: `<cross-session-message from="agmsgd">${marker}</cross-session-message>`,
  })}\n${JSON.stringify({
    type: 'attachment',
    attachment: { type: 'queued_command', prompt: `<cross-session-message from="agmsgd">${marker}</cross-session-message>` },
  })}\n`);
  assert.equal(observeTranscript(transcriptPath, nonce).state, 'delivered');
});

// No clock decides a pending nudge: whatever the transcript shows (or fails to
// show), it stays pending until the channel itself sees the socket or the
// seat's record change.
test('resolvePendingDelivery: nothing but a delivered nonce ends a pending nudge', () => {
  for (const observation of [
    { state: 'absent' },
    { state: 'held' },
    { state: 'unreadable', reason: 'transcript_missing' },
    { state: 'unreadable', reason: 'transcript_read_failed' },
  ]) {
    assert.equal(resolvePendingDelivery({ observation, ageMs: 10 ** 9 }).state, 'pending');
  }
  assert.equal(resolvePendingDelivery({ observation: { state: 'delivered' } }).state, 'confirmed');
});

function channelFixture(t, overrides = {}) {
  const root = temporaryDirectory(t);
  mkdirSync(path.join(root, 'run'));
  const db = openInstallDb(path.join(root, 'run', 'install.db'));
  ensureClaudeCodeChannelSchema(db);
  db.exec("UPDATE daemon_intent SET desired='on', op_gen=1");
  t.after(() => db.close());
  const state = { session: 'sess-1', socket: '/tmp/cc-socks/1.sock', sent: 0, hours: 0, alive: { state: 'ok' } };
  const channel = createClaudeCodeQueueChannel({
    db, installRoot: root, expectedOpGen: 1, env: { AGMSG_STORAGE_PATH: root },
    now: () => Date.UTC(2026, 9, 14) + state.hours * 3_600_000,
    log: () => {},
    listSeats: () => ({ state: 'ok', seats: [{ team: 'test', agent: 'alice', teamConfig: { agents: { alice: { registrations: [{ type: 'claude-code', project: root }] } } } }] }),
    readSession: () => ({ state: 'present', record: { team: 'test', agent: 'alice', type: 'claude-code', project: root, claude_config_dir: root, messaging_socket: state.socket, session: state.session } }),
    readDriver: () => ({ state: 'ok', driver: 'sqlite' }),
    openStore: () => ({ close() {} }),
    readSnapshot: () => ({ state: 'ok', unread: true, upTo: '{}' }),
    send: async () => { state.sent += 1; return { state: 'sent' }; },
    observe: () => ({ state: 'absent' }),
    socketAlive: () => state.alive,
    hostPlatform: 'linux',
    ...overrides,
  });
  return { db, channel, state, root };
}

test('an unconfirmed nudge is not sent again, however long the same session and socket stay up', async (t) => {
  const { db, channel, state } = channelFixture(t);
  await channel.pollOnce();
  for (const hours of [1, 9, 40]) {
    state.hours = hours;
    await channel.pollOnce();
  }
  // A check that could not run is not evidence the socket is gone.
  state.alive = { state: 'unreadable', reason: 'messaging_socket_stat_failed' };
  await channel.pollOnce();
  assert.equal(state.sent, 1);
  assert.equal(db.prepare("SELECT COUNT(*) AS n FROM beta_claude_queue WHERE state='pending'").get().n, 1);
  await channel.stop();
});

test('a nudge is sent again once the seat record names another session', async (t) => {
  const { db, channel, state } = channelFixture(t);
  await channel.pollOnce();
  state.session = 'sess-2';
  await channel.pollOnce();
  assert.equal(state.sent, 2);
  assert.deepEqual(db.prepare('SELECT state FROM beta_claude_queue ORDER BY id').all().map((row) => row.state), ['expired', 'pending']);
  await channel.stop();
});

test('a conversation stored under another project directory is found by its session id, only when exactly one matches', (t) => {
  const config = temporaryDirectory(t);
  const session = '0d7c2a52-5d4c-4d0f-8c0a-6b6f5f3f9a11';
  const expected = `${config}/projects/-proj-recorded/${session}.jsonl`;
  const elsewhere = `${config}/projects/-proj-started-elsewhere/${session}.jsonl`;
  mkdirSync(path.dirname(elsewhere), { recursive: true });
  writeFileSync(elsewhere, '');
  assert.equal(resolveTranscriptPath(config, '/proj/recorded', session), elsewhere);
  const second = `${config}/projects/-proj-another/${session}.jsonl`;
  mkdirSync(path.dirname(second), { recursive: true });
  writeFileSync(second, '');
  assert.equal(resolveTranscriptPath(config, '/proj/recorded', session), expected, 'two matches decide nothing');
  mkdirSync(path.dirname(expected), { recursive: true });
  writeFileSync(expected, '');
  assert.equal(resolveTranscriptPath(config, '/proj/recorded', session), expected, 'the recorded project wins when it has the file');
});

function deliveredRow(nonce) {
  return `${JSON.stringify({ type: 'user', origin: { kind: 'peer', body: `[agmsg:${nonce}]` } })}\n`;
}

function transcriptGrowth() {
  return `${JSON.stringify({ type: 'assistant', message: { content: 'context '.repeat(100_000) } })}\n`;
}

test('delivery survives more than 512 KiB of subsequent transcript growth and cold recovery', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  writeFileSync(file, deliveredRow('older-delivery') + transcriptGrowth());
  assert.equal(observeTranscript(file, 'older-delivery').state, 'delivered');
});

test('observer scans new bytes once, retains held evidence, and waits for a complete appended row', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const nonce = 'incremental';
  writeFileSync(file, transcriptGrowth());
  let bytes = 0;
  const observe = createTranscriptObserver({ read(...args) { const count = readSync(...args); bytes += count; return count; } });
  observe.begin(file, nonce);
  assert.equal(observe(file, nonce).state, 'absent');
  assert.equal(bytes, 2, 'begin and observe only check the byte before the EOF watermark');
  appendFileSync(file, `${JSON.stringify({ type: 'queue-operation', operation: 'enqueue', content: `[agmsg:${nonce}]` })}\n`);
  assert.equal(observe(file, nonce).state, 'absent', 'enqueue never confirms');
  appendFileSync(file, `${JSON.stringify({ type: 'system', subtype: 'informational', content: `[agmsg:${nonce}] held` })}\n`);
  assert.equal(observe(file, nonce).state, 'held');
  const scanned = bytes;
  assert.equal(observe(file, nonce).state, 'held');
  assert.equal(bytes, scanned + 1, 'unchanged polls only validate the checkpoint boundary');
  const row = deliveredRow(nonce);
  appendFileSync(file, row.slice(0, -4));
  assert.equal(observe(file, nonce).state, 'held');
  appendFileSync(file, row.slice(-4, -1));
  assert.equal(observe(file, nonce).state, 'held', 'JSON without its row terminator is incomplete');
  appendFileSync(file, '\n' + transcriptGrowth());
  assert.equal(observe(file, nonce).state, 'delivered');
});

test('cold recovery crosses giant rows and chunk boundaries, and then only scans appends', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const nonce = 'recover';
  writeFileSync(file, transcriptGrowth());
  let bytes = 0;
  const observe = createTranscriptObserver({ read(...args) { const count = readSync(...args); bytes += count; return count; } });
  assert.equal(observe(file, nonce).state, 'absent');
  assert.ok(bytes > 512 * 1024);
  const scanned = bytes;
  assert.equal(observe(file, nonce).state, 'absent');
  assert.equal(bytes, scanned + 1, 'unchanged polls only validate the checkpoint boundary');
  // Unicode and JSON escapes may straddle byte/chunk/append boundaries.
  const row = JSON.stringify({ type: 'attachment', attachment: { type: 'queued_command', prompt: '日本語 ' + `[agmsg:${nonce}]` } }).replace('[agmsg:', '\\u005bagmsg:') + '\n';
  const encoded = Buffer.from(row);
  const split = encoded.indexOf(Buffer.from('日本語')) + 1;
  appendFileSync(file, encoded.subarray(0, split));
  assert.equal(observe(file, nonce).state, 'absent');
  appendFileSync(file, encoded.subarray(split));
  assert.equal(observe(file, nonce).state, 'delivered');
});

test('observer resets held state after rotation or truncation and recovers evicted notices', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const nonce = 'replaced';
  const held = `${JSON.stringify({ type: 'system', subtype: 'informational', content: `[agmsg:${nonce}] held` })}\n`;
  const observe = createTranscriptObserver({ maxEntries: 1 });
  writeFileSync(file, held);
  assert.equal(observe(file, nonce).state, 'held');
  renameSync(file, file + '.old');
  writeFileSync(file, '{}\n');
  assert.equal(observe(file, nonce).state, 'absent');
  appendFileSync(file, held);
  assert.equal(observe(file, nonce).state, 'held');
  writeFileSync(file, '{}\n');
  assert.equal(observe(file, nonce).state, 'absent');
  appendFileSync(file, deliveredRow(nonce) + transcriptGrowth());
  assert.equal(observe(file, 'another-notice').state, 'absent');
  assert.equal(observe(file, nonce).state, 'delivered', 'eviction falls back to full recovery');
  observe.clear();
  assert.equal(observe(file, nonce).state, 'delivered', 'restart recovers historical delivery');
});

test('malformed JSON and lookalike fields never confirm delivery', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  for (const row of [
    '{"type":"user","origin":{"body":"[agmsg:strict]"},}\n',
    '{"type":"user","origin":{"body":"[agmsg:strict]"}}garbage\n',
    '{"type":"user","origin":{"body":"[agmsg:strict]"},"origin":{}}\n',
    JSON.stringify({ type: 'user', other: { origin: { body: '[agmsg:strict]' } } }) + '\n',
    JSON.stringify({ type: 'user', origin: { body: '[agmsg:strict]' }, other: [1, 2, true, null, -0.25e+3] }).slice(0, -1) + ',}\n',
  ]) {
    writeFileSync(file, row);
    assert.equal(observeTranscript(file, 'strict').state, 'absent', row);
  }
});

test('channel confirms delivery buried by growth and can send its next notice', async (t) => {
  let file;
  const { db, channel, state, root } = channelFixture(t, {
    observe: createTranscriptObserver(),
    send: async ({ nonce }) => {
      state.sent += 1;
      appendFileSync(file, deliveredRow(nonce) + transcriptGrowth());
      return { state: 'sent' };
    },
  });
  file = `${root}/projects/${root.replace(/\//g, '-')}/sess-1.jsonl`;
  mkdirSync(path.dirname(file), { recursive: true });
  writeFileSync(file, transcriptGrowth());
  await channel.pollOnce();
  assert.deepEqual(db.prepare('SELECT state FROM beta_claude_queue ORDER BY id').all().map((row) => row.state), ['confirmed']);
  await channel.pollOnce();
  assert.equal(state.sent, 2);
  assert.deepEqual(db.prepare('SELECT state FROM beta_claude_queue ORDER BY id').all().map((row) => row.state), ['confirmed', 'confirmed']);
  await channel.stop();
});

test('delivery fields retain their provenance and valid JSON scalars do not interfere', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const nonce = 'provenance';
  const marker = `[agmsg:${nonce}]`;
  for (const row of [
    { type: 'user', message: { content: marker } },
    { type: 'user', origin: { body: { text: marker } } },
    { type: 'attachment', attachment: { type: 'other', prompt: marker } },
    { type: 'attachment', other: { type: 'queued_command', prompt: marker } },
    { type: 'queue-operation', operation: 'enqueue', content: marker, origin: { body: marker } },
  ]) {
    writeFileSync(file, JSON.stringify(row) + '\n');
    assert.equal(observeTranscript(file, nonce).state, 'absent');
  }
  writeFileSync(file, JSON.stringify({ type: 'user', origin: { body: marker }, other: [0, -1, 0.25, 1e100, true, false, null, {}, []] }) + '\n');
  assert.equal(observeTranscript(file, nonce).state, 'delivered');
});

test('nesting guard rejects the whole over-deep row and resumes at the next row', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const nonce = 'depth-guard';
  const row = (depth) => `{"type":"user","origin":{"body":"[agmsg:${nonce}]"},"other":${'['.repeat(depth)}0${']'.repeat(depth)}}\n`;
  writeFileSync(file, row(127));
  assert.equal(observeTranscript(file, nonce).state, 'delivered', '128 container levels are accepted');
  writeFileSync(file, row(128));
  assert.equal(observeTranscript(file, nonce).state, 'absent', 'deeper evidence rows are deliberately rejected');
  appendFileSync(file, deliveredRow(nonce));
  assert.equal(observeTranscript(file, nonce).state, 'delivered', 'a rejected row never poisons subsequent evidence');
});

test('same-inode rewrite cannot splice cached partial JSON into delivery evidence', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const observe = createTranscriptObserver();
  writeFileSync(file, '{"type":"user","origin":{"body":"');
  assert.equal(observe(file, 'synthetic').state, 'absent');
  writeFileSync(file, '{"type":"tool","origin":{"body":"[agmsg:synthetic]"}}\n');
  assert.equal(observeTranscript(file, 'synthetic').state, 'absent');
  assert.equal(observe(file, 'synthetic').state, 'absent');
});

test('EOF watermark ignores a pre-existing partial row, then accepts the next complete row', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const observe = createTranscriptObserver();
  writeFileSync(file, 'unfinished-prefix');
  observe.begin(file, 'mid-row');
  appendFileSync(file, deliveredRow('mid-row'));
  assert.equal(observe(file, 'mid-row').state, 'absent', 'a suffix is never a standalone row');
  appendFileSync(file, deliveredRow('mid-row'));
  assert.equal(observe(file, 'mid-row').state, 'delivered');
});

test('same-inode replacement of a completed checkpoint must preserve its newline boundary', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const observe = createTranscriptObserver();
  writeFileSync(file, '{}\n');
  assert.equal(observe(file, 'boundary').state, 'absent');
  writeFileSync(file, 'bad' + deliveredRow('boundary'));
  assert.equal(observe(file, 'boundary').state, 'absent');
  assert.equal(observeTranscript(file, 'boundary').state, 'absent');
});

test('short reads preserve row boundaries and a read failure discards cached progress', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  writeFileSync(file, '{}\n' + deliveredRow('short-reads'));
  let fail = false;
  const observe = createTranscriptObserver({ read(fd, buffer, offset, length, position) {
    if (fail) throw Object.assign(new Error('synthetic read error'), { code: 'EIO' });
    return readSync(fd, buffer, offset, Math.min(length, 3), position);
  } });
  assert.equal(observe(file, 'short-reads').state, 'delivered');
  fail = true;
  assert.deepEqual(observe(file, 'short-reads'), { state: 'unreadable', reason: 'transcript_read_failed' });
  fail = false;
  writeFileSync(file, '{"type":"tool","origin":{"body":"[agmsg:short-reads]"}}\n');
  assert.equal(observe(file, 'short-reads').state, 'absent');
  appendFileSync(file, deliveredRow('short-reads'));
  assert.equal(observe(file, 'short-reads').state, 'delivered');
});

test('an incomplete UTF-8 suffix after a JSON object cannot be ignored at the newline', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  writeFileSync(file, Buffer.concat([Buffer.from(deliveredRow('utf8-tail').trimEnd()), Buffer.from([0xe3, 10])]));
  assert.equal(observeTranscript(file, 'utf8-tail').state, 'absent');
});

test('a same-size rewrite during short reads cannot synthesize delivery evidence', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const nonce = 'scan-rewrite';
  const user = deliveredRow(nonce);
  const tool = user.replace('"user"', '"tool"');
  assert.equal(Buffer.byteLength(user), Buffer.byteLength(tool));
  writeFileSync(file, user);
  let rewrite = true;
  const observe = createTranscriptObserver({ read(fd, buffer, offset, length, position) {
    const count = readSync(fd, buffer, offset, Math.min(length, 14), position);
    if (rewrite) { rewrite = false; writeFileSync(file, tool); }
    return count;
  } });
  assert.equal(observe(file, nonce).state, 'unreadable');
  assert.equal(observeTranscript(file, nonce).state, 'absent');
  assert.equal(observe(file, nonce).state, 'absent', 'unstable scan progress is discarded');
});

test('append during a scan defers confirmation and the next observation recovers delivery', (t) => {
  const file = path.join(temporaryDirectory(t), 'session.jsonl');
  const nonce = 'scan-append';
  writeFileSync(file, deliveredRow(nonce));
  let append = true;
  const observe = createTranscriptObserver({ read(fd, buffer, offset, length, position) {
    const count = readSync(fd, buffer, offset, length, position);
    if (append) { append = false; appendFileSync(file, '{}\n'); }
    return count;
  } });
  assert.equal(observe(file, nonce).state, 'unreadable', 'even a delivered early row requires a stable scan');
  assert.equal(observe(file, nonce).state, 'delivered', 'recovery must include the row from the discarded scan');
});
