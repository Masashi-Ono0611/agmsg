import assert from 'node:assert/strict';
import { chmodSync, cpSync, mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFileSync, spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import test from 'node:test';
import { openInstallDb } from '../scripts/daemon/db.mjs';
import { ensureCodexChannelSchema } from '../scripts/daemon/channels/codex-queue-store.mjs';

const repo = fileURLToPath(new URL('../', import.meta.url));
const thread = '01a101a5-3599-7d62-a149-4c8182bcf007';
function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'daemon-status-display-'));
  cpSync(join(repo, 'scripts'), join(root, 'scripts'), { recursive: true });
  mkdirSync(join(root, 'run'));
  mkdirSync(join(root, 'teams/test'), { recursive: true });
  const db = openInstallDb(join(root, 'run/install.db'));
  ensureCodexChannelSchema(db);
  t.after(() => db.close());
  const config = { name: 'test', agents: { alice: { registrations: [
    { type: 'codex', project: '/first' }, { type: 'codex', project: '/second' },
  ] } } };
  writeFileSync(join(root, 'teams/test/config.json'), JSON.stringify(config));
  writeFileSync(join(root, 'run/role-session.test__alice'), `type=codex\nsession=${thread}\ncodex_home=${root}\nupdated_at=${new Date().toISOString()}\n`);
  // Read-only liveness evidence points at this test process, never a real seat.
  writeFileSync(join(root, 'run/codex-bridge.test.alice.pid'), `${process.pid}\n`);
  writeFileSync(join(root, 'run/codex-bridge.test.alice.meta'), `pid=${process.pid}\ntype=codex\nproject=/second\n`);
  const status = (...flags) => execFileSync('bash', [join(root, 'scripts/daemon.sh'), 'status', ...flags], {
    encoding: 'utf8', env: { ...process.env, AGMSG_CONFIG: join(root, 'config.json') },
  });
  return { root, db, config, status };
}

test('daemon status defaults to readable text and emits only JSON with --json', t => {
  const { status } = fixture(t);
  const text = status();
  assert.match(text, /agmsgd has never been started/);
  assert.doesNotMatch(text, /"exitCode"|"codex_seats"/);
  const json = JSON.parse(status('--json'));
  assert.equal(json.exitCode, 0);
  assert.equal(json.codex_seats.state, 'ok');
});

test('multiple project registrations produce one seat line', t => {
  const { status } = fixture(t);
  assert.equal(status().split('\n').filter(line => line.startsWith('Codex test/alice:')).length, 1);
});

test('bridge display uses every registered project and does not repeat a contradictory cached verdict', t => {
  const { db, status } = fixture(t);
  db.prepare('INSERT INTO beta_codex_seat VALUES (?, ?, ?, ?, ?, ?)').run(JSON.stringify(['test', 'alice']), thread, '/profile', 'blocked', 'bridge_state_unknown', new Date().toISOString());
  const text = status();
  assert.match(text, /bridge process running/);
  assert.doesNotMatch(text, /bridge.*unknown|bridge could not be verified/);
});

test('old destinations are counted by default and available with --all after current seats', t => {
  const { root, db, config, status } = fixture(t);
  config.agents.ancient = { registrations: [{ type: 'codex', project: '/old' }] };
  writeFileSync(join(root, 'teams/test/config.json'), JSON.stringify(config));
  writeFileSync(join(root, 'run/role-session.test__ancient'), 'updated_at=2020-01-01T00:00:00Z\n');
  const text = status();
  assert.doesNotMatch(text, /Codex test\/ancient:/);
  assert.match(text, /1 older destination records hidden.*--all/);
  const all = status('--all');
  assert.match(all, /Codex test\/ancient:/);
  assert.ok(all.indexOf('Codex test/alice:') < all.indexOf('Codex test/ancient:'));
  db.prepare("INSERT INTO beta_codex_queue (seat,codex_home,thread,up_to,nonce,state,children,created_at) VALUES (?, ?, ?, '{}', 'old-pending', 'pending', 'incomplete', '2020-01-01T00:00:00Z')")
    .run(JSON.stringify(['test', 'ancient']), root, thread);
  assert.match(status(), /Codex test\/ancient:.*waiting for delivery confirmation/);
});

test('never-recorded seats are not counted as older destinations, but pending evidence stays visible', t => {
  const { root, db, config, status } = fixture(t);
  config.agents = { fresh: { registrations: [{ type: 'codex', project: '/fresh' }] } };
  writeFileSync(join(root, 'teams/test/config.json'), JSON.stringify(config));
  for (const flags of [[], ['--all']]) {
    const text = status(...flags);
    assert.doesNotMatch(text, /older destination|Older destination|Codex test\/fresh:/);
    assert.match(text, /agmsgd has never been started/);
  }
  db.prepare("INSERT INTO beta_codex_queue (seat,codex_home,thread,up_to,nonce,state,children,created_at) VALUES (?, ?, ?, '{}', 'fresh-pending', 'pending', 'incomplete', '2020-01-01T00:00:00Z')")
    .run(JSON.stringify(['test', 'fresh']), root, thread);
  assert.match(status(), /Codex test\/fresh: no destination record;.*waiting for delivery confirmation/);
});

test('installer restart guidance tells users to resume, refresh their destination, and check status', () => {
  const source = readFileSync(join(repo, 'install.sh'), 'utf8');
  const fn = source.match(/agmsg_install_restart_guidance\(\) \{[\s\S]*?\n\}/)[0];
  const output = execFileSync('bash', ['-c', `${fn}\nSKILL_DIR=/tmp/isolated-install\nagmsg_install_restart_guidance`], { encoding: 'utf8' });
  assert.match(output, /close and reopen/);
  assert.match(output, /Resume the same conversation.*actas/);
  assert.match(output, /\/tmp\/isolated-install\/scripts\/agmsg daemon status/);
  assert.match(output, /does not enable agmsgd/);
  assert.equal((source.match(/^  agmsg_install_restart_guidance$/gm) ?? []).length, 1);
  assert.equal((source.match(/^agmsg_install_restart_guidance$/gm) ?? []).length, 1);
});

test('pending confirmations remain discoverable after a seat registration is removed', t => {
  const { db, root, status } = fixture(t);
  db.prepare("INSERT INTO beta_codex_queue (seat,codex_home,thread,up_to,nonce,state,children,created_at) VALUES (?, ?, ?, '{}', 'removed-pending', 'pending', 'incomplete', '2020-01-01T00:00:00Z')")
    .run(JSON.stringify(['test', 'removed']), root, thread);
  const summary = status();
  assert.match(summary, /Pending confirmations for unregistered seats: 1.*--all/);
  assert.doesNotMatch(summary, /Codex test\/removed:/);
  assert.match(status('--all'), /Codex test\/removed: unregistered; past destination; confirmation pending since 2020-01-01T00:00:00Z/);
  assert.equal(JSON.parse(status('--json')).codex_pending.length, 1);
  assert.equal(db.prepare('SELECT state FROM beta_codex_queue').get().state, 'pending');
});

test('status suppresses managed SQLite experimental notices but retains ordinary warnings', t => {
  const { root } = fixture(t);
  const entry = join(root, 'scripts/daemon/status.mjs');
  writeFileSync(entry, readFileSync(entry, 'utf8') + '\nprocess.emitWarning("SQLite is an experimental feature", "ExperimentalWarning");\nprocess.emitWarning("fixture ordinary warning");\n');
  const result = spawnSync('bash', [join(root, 'scripts/daemon.sh'), 'status'], { encoding: 'utf8', env: { ...process.env, AGMSG_CONFIG: join(root, 'config.json') } });
  assert.equal(result.status, 0);
  assert.doesNotMatch(result.stderr, /ExperimentalWarning|SQLite is an experimental/);
  assert.match(result.stderr, /fixture ordinary warning/);
});

test('launcher ignores temporary npx PATH entries and still gives the permanent command check', () => {
  const root = mkdtempSync(join(tmpdir(), 'launcher-npx-status-'));
  const transient = join(root, '_npx/temporary/node_modules/.bin');
  mkdirSync(transient, { recursive: true });
  const entry = join(transient, 'agmsg');
  writeFileSync(entry, '#!/bin/sh\n# agmsg npm bootstrapper\n');
  chmodSync(entry, 0o755);
  const output = execFileSync('bash', ['-c', 'source "$1"; agmsg_launcher_install "$2"', '_', join(repo, 'scripts/lib/agmsg-launcher.sh'), join(root, 'install')], {
    encoding: 'utf8', env: { ...process.env, AGMSG_BIN_DIR: join(root, 'bin'), PATH: `${transient}:/usr/bin:/bin:/usr/sbin:/sbin` },
  });
  assert.doesNotMatch(output, /currently resolves to|_npx|older npm/);
  assert.match(output, /open a new terminal and run: command -v agmsg/);
});
