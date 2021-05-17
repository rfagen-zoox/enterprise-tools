#!/usr/bin/env node --max-old-space-size=8192
'use strict';

const _ = require('lodash');
const commandLineArgs = require('command-line-args');
const es = require('event-stream');
const fs = require('fs');
const forEachLimit = require('async-co/eachLimit');
const forEachOfLimit = require('async-co/eachOfLimit');
const getUsage = require('command-line-usage');
const NodeFire = require('nodefire');
const PromiseReadable = require('promise-readable');

const commandLineOptions = [
  {name: 'input', alias: 'o', typeLabel: '[underline]{data.json}',
    description: 'Input JSON file with extracted data.'},
  {name: 'admin', alias: 'a', typeLabel: '[underline]{github:NNNN}',
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

const data = {};
let pace, repoEntries;

async function load() {
  console.log('Reading data file...');
  await readData();

  console.log('Uploading data to Firebase...');
  repoEntries = _(data.repositories)
    .map((org, orgName) => _.map(org, (repo, repoName) => ({owner: orgName, repo: repoName})))
    .flattenDeep().value();
  pace = require('pace')(1 + _.size(data.reviews) + _.size(data.users) + _.size(repoEntries));

  await db.auth(process.env.REVIEWABLE_FIREBASE_AUTH);
  await Promise.all([loadOrganizations(), loadRepositories()]);
  await loadUsers();
  await loadReviews();
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

async function readData() {
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
  await new PromiseReadable(reader).once('end');
}

async function loadOrganizations() {
  await db.child('organizations').update(data.organizations);
  pace.op();
}

async function loadRepositories() {
  await forEachLimit(repoEntries, 10, async ({owner, repo}) => {
    // owner and repo are already escaped
    await db.child(`repositories/${owner}/${repo}`).update(data.repositories[owner][repo]);
    pace.op();
  });
}

async function loadReviews() {
  await forEachOfLimit(data.reviews, 25, async (review, reviewKey) => {
    const rdb = db.scope({reviewKey});
    await rdb.child('reviews/:reviewKey').update(review);
    const linemap = data.linemaps[reviewKey], filemap = data.filemaps[reviewKey];
    await Promise.all([
      linemap ? rdb.child('linemaps/:reviewKey').set(linemap) : Promise.resolve(),
      filemap ? rdb.child('filemaps/:reviewKey').set(filemap) : Promise.resolve()
    ]);
    const syncOptions = {
      userKey: args.admin, prNumber: review.core.pullRequestId,
      owner: _.toLower(review.core.ownerName), repo: _.toLower(review.core.repoName),
      updateReview: true, syncComments: true, syncStatus: true, mustSucceed: true,
      overrideBadge: true, timestamp: NodeFire.ServerValue.TIMESTAMP
    };
    await rdb.child(
      'queues/githubPullRequestSync/:owner|:repo|:prNumber|:userKey', syncOptions
    ).update(syncOptions);
    pace.op();
  });
}

async function loadUsers() {
  await forEachOfLimit(data.users, 25, async (user, userKey) => {
    await Promise.all([
      db.child('users/:userKey', {userKey}).update(user),
      db.child('queues/requests').push({
        action: 'fillUserProfile', userKey: args.admin, userId: userKey.replace(/github:/, '')
      })
    ]);
    pace.op();
  });
}
