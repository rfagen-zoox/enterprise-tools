#!/usr/bin/env node
'use strict';

global.Promise = require('bluebird');
Promise.co = require('co');
const fs = require('fs');
const commandLineArgs = require('command-line-args');
const getUsage = require('command-line-usage');
const _ = require('lodash');
const eachLimit = require('async-co/eachLimit');
const eachOfLimit = require('async-co/eachOfLimit');
const NodeFire = require('nodefire');
const requireEnvVars = require('./lib/requireEnvVars.js');

NodeFire.setCacheSize(0);

const commandLineOptions = [
  {name: 'repos', alias: 'r', typeLabel: '[underline]{repos.json}',
   description: 'A file with a JSON array of "owner/repo" repo names to extract (required).'},
  {name: 'users', alias: 'u', typeLabel: '[underline]{users.json}',
   description:
    'A file with a JSON object of {"github:MMMM": "github:NNNN"} user id mappings (required).'},
  {name: 'ghost', alias: 'g', typeLabel: '[underline]{github:NNNN}',
   description: 'A user id to substitute instead of no mapping is found for an old user.'},
  {name: 'output', alias: 'o', typeLabel: '[underline]{data.json}',
   description: 'Output JSON file for extracted data (required).'},
  {name: 'help', alias: 'h', type: Boolean,
   description: 'Display these usage instructions.'}
];

const usageSpec = [
  {header: 'Data extraction tool',
   content:
    'Extracts all data related to a set of repos from a Reviewable datastore, in preparation for ' +
    'transforming and loading it into another datastore.'
  },
  {header: 'Options', optionList: commandLineOptions}
];

const args = commandLineArgs(commandLineOptions);
if (args.help) {
  console.log(getUsage(usageSpec));
  process.exit(0);
}
for (let property of ['repos', 'users', 'output']) {
  if (!(property in args)) throw new Error('Missing required option: ' + property + '.');
}

requireEnvVars('REVIEWABLE_FIREBASE', 'REVIEWABLE_FIREBASE_AUTH');

const repoNames =
  _(JSON.parse(fs.readFileSync(args.repos))).map(key => key.toLowerCase()).uniq().value();
const userMap = JSON.parse(fs.readFileSync(args.users));

const pace = require('pace')(1);

const db = new NodeFire(`https://${process.env.REVIEWABLE_FIREBASE}.firebaseio.com`);

const data = {};
let reviewKeys;
let ghostedUserKeys = [];

Promise.co(function*() {
  yield db.auth(process.env.REVIEWABLE_FIREBASE_AUTH);
  yield [extractRepositories(), extractOrganizations()];
  reviewKeys = listReviewKeys();
  yield [extractReviews(), extractUsers()];
  fs.writeFileSync(args.output, JSON.stringify(data));
  pace.op();

  if (ghostedUserKeys.length) {
    ghostedUserKeys = _.uniq(ghostedUserKeys);
    console.log(`${ghostedUserKeys.length} users could not be mapped over:`);
    const usernames = yield eachLimit(ghostedUserKeys, 5, function*(userKey) {
      const user = yield db.child('users/:userKey/core/public', {userKey}).get();
      return user ? user.username : ` user ${userKey.replace(/github:/, '')}`;
    });
    console.log(_(usernames).sortBy().join('\n'));
  }
}).then(() => {
  process.exit(0);
}, e => {
  console.log();
  if (e.errors) console.log(e.errors);
  console.log(e.stack);
  process.exit(1);
});


function *extractOrganizations() {
  const orgNames = _(repoNames).map(name => name.replace(/\/.*/, '')).uniq().value();
  if (!orgNames.length) return;
  pace.total += orgNames.length;
  data.organizations = {};
  yield eachLimit(orgNames, 5, function*(org) {
    const organization = yield db.child('organizations/:org', {org}).get();
    if (organization) {
      data.organizations[NodeFire.escape(org)] = mapAllUserKeys(organization);
    }
    pace.op();
  });
}

function *extractRepositories() {
  if (!repoNames.length) return;
  pace.total += repoNames.length;
  data.repositories = {};
  yield eachLimit(repoNames, 10, function*(repoName) {
    const [owner, repo] = repoName.split('/');
    let repository = yield db.child('repositories/:owner/:repo', {owner, repo}).get();
    if (repository) {
      repository.core = _.omit(
        repository.core, 'id', 'connection', 'connector', 'reviewableBadge', 'errorCode', 'error',
        'hookEvents'
      );
      repository = _.omit(repository, 'adminUserKeys', 'current', 'issues', 'protection');
      data.repositories[NodeFire.escape(owner)] = data.repositories[NodeFire.escape(owner)] || {};
      data.repositories[NodeFire.escape(owner)][NodeFire.escape(repo)] = mapAllUserKeys(repository);
    }
    pace.op();
  });
}

function listReviewKeys() {
  return _(data.repositories)
    .map(organization => _.map(organization, repository => [
      _.values(repository.pullRequests), _.values(repository.oldPullRequests)
    ]))
    .flattenDeep()
    .uniq()
    .value();
}

function *extractReviews() {
  pace.total += reviewKeys.length;
  data.reviews = {};
  data.linemaps = {};
  data.filemaps = {};
  yield eachLimit(reviewKeys, 25, function*(reviewKey) {
    const rdb = db.scope({reviewKey});
    const {review, linemap, filemap} = yield {
      review: rdb.child('reviews/:reviewKey').get(),
      linemap: rdb.child('linemaps/:reviewKey').get(),
      filemap: rdb.child('filemaps/:reviewKey').get()
    };
    review.core = _.omit(review.core, 'lastSweepTimestamp');
    review.discussions = _.pick(review.discussions, discussion => {
      discussion.comments =
        _.pick(discussion.comments, (comment, commentKey) => !/^gh-/.test(commentKey));
      return !_.isEmpty(discussion.comments);
    });
    if (_.isEmpty(review.discussions)) delete review.discussions;
    delete review.gitHubComments;
    review.sentiments = _.pick(review.sentiments, sentiment => {
      sentiment.comments =
        _.pick(sentiment.comments, (comment, commentKey) => !/^gh-/.test(commentKey));
      return !_.isEmpty(sentiment.comments);
    });
    if (_.isEmpty(review.sentiments)) delete review.sentiments;
    data.reviews[reviewKey] = mapAllUserKeys(review);
    if (linemap) data.linemaps[reviewKey] = linemap;
    if (filemap) data.filemaps[reviewKey] = filemap;
    pace.op();
  });
}

function *extractUsers() {
  pace.total += _.size(userMap);
  data.users = {};
  yield eachOfLimit(userMap, 25, function*(newUserKey, oldUserKey) {
    let user = yield db.child('users/:oldUserKey', {oldUserKey}).get();
    user = _.omit(user, 'stripe', 'enrollments', 'index', 'notifications');
    delete user.core;
    if (user.settings) delete user.settings.dismissals;
    if (user.state) user.state = _.pick(user.state, reviewKeys);
    data.users[newUserKey] = mapAllUserKeys(user);
    pace.op();
  });
}

function mapAllUserKeys(object) {
  if (_.isString(object)) {
    if (/^github:\d+$/.test(object)) return mapUserKey(object);
    if (/^github:\d+(\s*,\s*github:\d+)*$/.test(object)) {
      return _(object).split(/\s*,\s*/).map(mapUserKey).uniq().join(',');
    }
  } else if (_.isObject(object)) {
    for (const key in object) {
      const value = mapAllUserKeys(object[key]);
      const newKey = mapAllUserKeys(key);
      if (key !== newKey) delete object[key];
      if (_.isObject(value) && _.isEmpty(value)) {
        delete object[key];
      } else {
        object[newKey] = value;
      }
    }
  }
  return object;
}

function mapUserKey(userKey) {
  const newUserKey = userMap[userKey] || args.ghost;
  if (!newUserKey) throw new Error(`User not mapped and no ghost specified: ${userKey}`);
  if (newUserKey === args.ghost) ghostedUserKeys.push(userKey);
  return newUserKey;
}
