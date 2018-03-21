#!/usr/bin/env node --max-old-space-size=8192
'use strict';

global.Promise = require('bluebird');
Promise.co = require('co');
const commandLineArgs = require('command-line-args');
const getUsage = require('command-line-usage');
const _ = require('lodash');
const Firebase = require('firebase');
const NodeFire = require('nodefire');
const requireEnvVars = require('./lib/requireEnvVars.js');
const util = require('util');

NodeFire.setCacheSize(0);

const commandLineOptions = [
  {name: 'path', alias: 'p', type: String, defaultOption: true,
   description: 'The path in Firebase from which to read data.  You can omit the leading slash.'},
  {name: 'help', alias: 'h', type: Boolean,
   description: 'Display these usage instructions.'}
];

const usageSpec = [
  {header: 'Data readout tool',
   content:
    'Reads a given path from Firebase and prints the result, decrypting if necessary.  Relies on ' +
    'REVIEWABLE_FIREBASE, REVIEWABLE_FIREBASE_AUTH, and REVIEWABLE_ENCRYPTION_AES_KEY being set.'
  },
  {header: 'Options', optionList: commandLineOptions}
];

const args = commandLineArgs(commandLineOptions);
if (args.help) {
  console.log(getUsage(usageSpec));
  process.exit(0);
}
for (let property of ['path']) {
  if (!(property in args)) throw new Error('Missing required option: ' + property + '.');
}

requireEnvVars('REVIEWABLE_FIREBASE', 'REVIEWABLE_FIREBASE_AUTH');

if (process.env.REVIEWABLE_ENCRYPTION_AES_KEY) {
  require('firecrypt');
  Firebase.initializeEncryption(
    {
      algorithm: 'aes-siv', key: process.env.REVIEWABLE_ENCRYPTION_AES_KEY,
      cacheSize: 50 * 1048576
    },
    require('./rules_firecrypt.json')
  );
}

const db = new NodeFire(`https://${process.env.REVIEWABLE_FIREBASE}.firebaseio.com`);

Promise.co(function*() {
  args.path = args.path.replace(/^\//, '');
  yield db.auth(process.env.REVIEWABLE_FIREBASE_AUTH);
  const value = yield db.child(args.path).get();
  console.log(util.inspect(value, {depth: null}));
}).then(() => {
  process.exit(0);
}, e => {
  console.log(e);
  process.exit(1);
});
