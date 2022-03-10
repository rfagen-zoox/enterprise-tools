#!/usr/bin/env node

import _ from 'lodash';
import * as fs from 'fs';
import es from 'event-stream';
import commandLineArgs from 'command-line-args';
import getUsage from 'command-line-usage';
import nodefireModule from 'nodefire';
import {PromiseReadable} from 'promise-readable';
import Pace from 'pace';
import {Throttle} from 'stream-throttle';

const NodeFire = nodefireModule.default;

const commandLineOptions = [
  {name: 'input', alias: 'i', typeLabel: '{underline data.ndjson}',
    description: 'Input ndJSON file with extracted data.'},
  {name: 'admin', alias: 'a', typeLabel: '{underline github:NNNN}',
    description: 'The user id of a GHE user with valid OAuth credentials in Reviewable.'},
  {name: 'help', alias: 'h', type: Boolean,
    description: 'Display these usage instructions.'}
];

const usageSpec = [
  {header: 'Data upload tool',
    content:
      'Uploads all data related to a set of repos (previously extracted with extract_data.js) to ' +
      'a Reviewable datastore and resyncs some data with GitHub Enterprise.'
  },
  {header: 'Options', optionList: commandLineOptions}
];

const args = commandLineArgs(commandLineOptions);
if (args.help) {
  console.log(getUsage(usageSpec));
  process.exit(0);
}
for (const property of ['input', 'admin']) {
  if (!(property in args)) {
    console.log('Missing required option: ' + property + '.');
    process.exit(1);
  }
}

if (!process.env.REVIEWABLE_ENCRYPTION_AES_KEY) {
  console.log('WARNING: not encrypting uploaded data as REVIEWABLE_ENCRYPTION_AES_KEY not given');
}

async function load() {
  await import('./lib/loadFirebase.js');

  let sizeRead = 0;
  let fatalError;

  const pace = Pace(fs.statSync(args.input).size);
  const reader = fs.createReadStream(args.input)
    .pipe(new Throttle({rate: 200000, chunksize: 50000}))
    .pipe(es.mapSync(chunk => {
      sizeRead += chunk.length;
      pace.op(sizeRead);
      return chunk;
    }))
    .pipe(es.split())
    .pipe(es.parse({error: true}))
    .pipe(es.map(async (item, callback) => {
      await processLine(item);
      callback();
    }))
    .on('error', error => {
      fatalError = error;
    });
  await new PromiseReadable(reader).once('end');
  if (fatalError) throw fatalError;
}

load().then(() => {
  process.exit(0);
}, e => {
  console.log();
  if (e.errors) console.log(e.errors);
  console.log(e.stack);
  if (e.extra && e.extra.debug) console.log(e.extra.debug);
  process.exit(1);
});


async function processLine([key, value]) {
  if (!_.isEmpty(value)) await db.child(key).update(value);
  if (_.startsWith(key, 'reviews/')) {
    const syncOptions = {
      userKey: args.admin, prNumber: value.core.pullRequestId,
      owner: _.toLower(value.core.ownerName), repo: _.toLower(value.core.repoName),
      updateReview: true, syncComments: true, syncStatus: true, mustSucceed: true,
      overrideBadge: true, timestamp: NodeFire.SERVER_TIMESTAMP
    };
    await db.child(
      'queues/githubPullRequestSync/:owner|:repo|:prNumber|:userKey', syncOptions
    ).update(syncOptions);
  } else if (_.startsWith(key, 'users/')) {
    await db.child('queues/requests').push({
      action: 'fillUserProfile', userKey: args.admin, userId: key.match(/users\/github:(\d+)/)[1]
    });
  }
}

