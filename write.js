#!/usr/bin/env node

import commandLineArgs from 'command-line-args';
import getUsage from 'command-line-usage';
import fs from 'fs';

const commandLineOptions = [
  {name: 'path', alias: 'p', type: String,
    description: 'The path in Firebase to which to write data.  You can omit the leading slash.'},
  {name: 'file', alias: 'f', type: String,
    description: 'The JSON file containing the data'},
  {name: 'help', alias: 'h', type: Boolean,
    description: 'Display these usage instructions.'}
];

const usageSpec = [
  {header: 'Data readout tool',
    content:
      'Writes stdin stream to a given path in Firebase, encrypting if necessary. ' +
      'REVIEWABLE_FIREBASE_URL, REVIEWABLE_FIREBASE_CREDENTIALS_FILE, and ' +
      'REVIEWABLE_ENCRYPTION_AES_KEY must be set.'
  },
  {header: 'Options', optionList: commandLineOptions}
];

const args = commandLineArgs(commandLineOptions);
if (args.help) {
  console.log(getUsage(usageSpec));
  process.exit(0);
}
for (const property of ['path', 'file']) {
  if (!(property in args)) {
    console.log('Missing required option: ' + property + '.');
    process.exit(1);
  }
}

async function write() {
  await import('./lib/loadFirebase.js');
  args.path = args.path.replace(/^\//, '');
  const value = JSON.parse(fs.readFileSync(args.file));
  await db.child(args.path).set(value);
  console.log('Done');
}

write().then(() => {
  process.exit(0);
}, e => {
  console.log(e);
  process.exit(1);
});
