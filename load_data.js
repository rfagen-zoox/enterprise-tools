#!/usr/bin/env node --max-old-space-size=8192
'use strict';

global.Promise = require('bluebird');
Promise.co = require('co');
const fs = require('fs');
const es = require('event-stream');
const commandLineArgs = require('command-line-args');
const getUsage = require('command-line-usage');
const _ = require('lodash');
const eachLimit = require('async-co/eachLimit');
const eachOfLimit = require('async-co/eachOfLimit');
const Firebase = require('firebase');
const NodeFire = require('nodefire');
const PromiseReadable = require('promise-readable');
const requireEnvVars = require('./lib/requireEnvVars.js');

NodeFire.setCacheSize(0);

const commandLineOptions = [
  {name: 'input', alias: 'o', typeLabel: '[underline]{data.json}',
   description: 'Input JSON file with extracted data (required).'},
  {name: 'admin', alias: 'a', typeLabel: '[underline]{github:NNNN}',
   description: 'The user id of a GHE user with valid OAuth credentials in Reviewable (required).'},
  {name: 'help', alias: 'h', type: Boolean,
   description: 'Display these usage instructions.'}
];

const usageSpec = [
  {header: 'Data upload tool',
   content:
    'Uploads all data related to a set of repos (previously extracted with extract_data.js) to a ' +
    'Reviewable datastore and resyncs some data with GitHub Enterprise.'
  },
  {header: 'Options', optionList: commandLineOptions}
];

const args = commandLineArgs(commandLineOptions);
if (args.help) {
  console.log(getUsage(usageSpec));
  process.exit(0);
}
for (let property of ['input', 'admin']) {
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
} else {
  console.log('WARNING: not encrypting uploaded data as REVIEWABLE_ENCRYPTION_AES_KEY not given');
}

const data = {};
let pace, repoEntries;
const db = new NodeFire(`https://${process.env.REVIEWABLE_FIREBASE}.firebaseio.com`);

Promise.co(function*() {
  console.log('Reading data file...');
  yield readData();

  console.log('Uploading data to Firebase...');
  repoEntries = _(data.repositories)
    .map((org, orgName) => _.map(org, (repo, repoName) => ({owner: orgName, repo: repoName})))
    .flattenDeep().value();
  pace = require('pace')(1 + _.size(data.reviews) + _.size(data.users) + _.size(repoEntries));

  yield db.auth(process.env.REVIEWABLE_FIREBASE_AUTH);
  yield [loadOrganizations(), loadRepositories()];
  yield loadUsers();
  yield loadReviews();
}).then(() => {
  process.exit(0);
}, e => {
  console.log();
  if (e.errors) console.log(e.errors);
  console.log(e.stack);
  if (e.extra && e.extra.debug) console.log(e.extra.debug);
  process.exit(1);
});

function *readData() {
  pace = require('pace')(fs.statSync(args.input).size);
  let sizeRead = 0;
  const reader = fs.createReadStream(args.input)
    .pipe(es.mapSync(chunk => {
      sizeRead += chunk.length;
      pace.op(sizeRead);
      return chunk;
    }))
    .pipe(es.split())
    .pipe(es.mapSync(line => {
      const match = line.match(/^\s*,?\s*("[^"]+"):(.*)$/);
      if (!match) return;
      const path = JSON.parse(match[1]).split('/');
      let item = data;
      for (const segment of path.slice(0, -1)) {
        if (!item[segment]) item[segment] = {};
        item = item[segment];
      }
      item[_.last(path)] = JSON.parse(match[2]);
    }));
  yield new PromiseReadable(reader).once('end');
}

function *loadOrganizations() {
  yield db.child('organizations').update(data.organizations);
  pace.op();
}

function *loadRepositories() {
  yield eachLimit(repoEntries, 10, function*({owner, repo}) {
    // owner and repo are already escaped
    yield db.child(`repositories/${owner}/${repo}`).update(data.repositories[owner][repo]);
    pace.op();
  });
}

function *loadReviews() {
  yield eachOfLimit(data.reviews, 25, function*(review, reviewKey) {
    const rdb = db.scope({reviewKey});
    yield rdb.child('reviews/:reviewKey').update(review);
    const linemap = data.linemaps[reviewKey], filemap = data.filemaps[reviewKey];
    yield [
      linemap ? rdb.child('linemaps/:reviewKey').set(linemap) : Promise.resolve(),
      filemap ? rdb.child('filemaps/:reviewKey').set(filemap) : Promise.resolve()
    ];
    const syncOptions = {
      userKey: args.admin, prNumber: review.core.pullRequestId,
      owner: review.core.ownerName.toLowerCase(), repo: review.core.repoName.toLowerCase(),
      updateReview: true, syncComments: true, syncStatus: true, mustSucceed: true,
      overrideBadge: true, timestamp: NodeFire.ServerValue.TIMESTAMP
    };
    yield rdb.child(
      'queues/githubPullRequestSync/:owner|:repo|:prNumber|:userKey', syncOptions
    ).update(syncOptions);
    pace.op();
  });
}

function *loadUsers() {
  yield eachOfLimit(data.users, 25, function*(user, userKey) {
    yield [
      db.child('users/:userKey', {userKey}).update(user),
      db.child('queues/requests').push({
        action: 'fillUserProfile', userKey: args.admin, userId: userKey.replace(/github:/, '')
      })
    ];
    pace.op();
  });
}
