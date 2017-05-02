#!/usr/bin/env node
'use strict';

global.Promise = require('bluebird');
Promise.co = require('co');
require('any-promise/register/bluebird');
const fs = require('fs');
const commandLineArgs = require('command-line-args');
const getUsage = require('command-line-usage');
const _ = require('lodash');
const eachLimit = require('async-co/eachLimit');
const eachOfLimit = require('async-co/eachOfLimit');
const eachOf = require('async-co/eachOf');
const NodeFire = require('nodefire');
const PromiseWritable = require('promise-writable');
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

const userMap = JSON.parse(fs.readFileSync(args.users));
const repoNames =
  _(JSON.parse(fs.readFileSync(args.repos))).map(key => key.toLowerCase()).uniq().value();
const groupedRepoNames = _.groupBy(repoNames, name => name.split('/')[0]);
const orgNames = _(repoNames).map(name => name.replace(/\/.*/, '')).uniq().value();

const out = new PromiseWritable(fs.createWriteStream(args.output));
out.stream.setMaxListeners(Infinity);

const pace = require('pace')(1 + orgNames.length + repoNames.length + _.size(userMap));

const db = new NodeFire(`https://${process.env.REVIEWABLE_FIREBASE}.firebaseio.com`);

let reviewKeys = [];
let ghostedUsers = [];

Promise.co(function*() {
  yield db.auth(process.env.REVIEWABLE_FIREBASE_AUTH);
  yield writeCollection(function *writeTopLevelObject() {
    yield extractOrganizations();
    yield extractRepositories();
    reviewKeys = _.uniq(reviewKeys);
    pace.total += 3 * reviewKeys.length;
    yield extractUsers();
    yield extractReviews();
    yield extractLinemaps();
    yield extractFilemaps();
  });
  yield out.end();
  pace.op();

  if (ghostedUsers.length) {
    ghostedUsers = _.uniq(ghostedUsers, false, item => item.userKey);
    console.log(`${ghostedUsers.length} users could not be mapped over:`);
    const users = yield eachLimit(ghostedUsers, 5, function*(item) {
      const user = yield db.child('users/:userKey/core/public', {userKey: item.userKey}).get();
      return {
        username: user ? user.username : ` user ${item.userKey.replace(/github:/, '')}`,
        context: item.context
      };
    });
    console.log(
      _(users)
        .sortBy(user => user.username.toLowerCase())
        .map(user => `${user.username} @ ${user.context}`)
        .join('\n')
    );
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
  if (!orgNames.length) return;
  yield writeCollection('organizations', function*() {
    yield eachLimit(orgNames, 5, function*(org) {
      const organization = yield db.child('organizations/:org', {org}).get();
      if (organization) {
        yield writeItem(
          NodeFire.escape(org), mapAllUserKeys(organization, `/organizations/${org}`));
      }
      pace.op();
    });
  });
}

function *extractRepositories() {
  if (!repoNames.length) return;
  yield writeCollection('repositories', function*() {
    yield eachOf(groupedRepoNames, function*(repoNamesGroup, orgName) {
      yield writeCollection(NodeFire.escape(orgName), function*() {
        yield eachLimit(repoNamesGroup, 10, function*(repoName) {
          const [owner, repo] = repoName.split('/');
          let repository = yield db.child('repositories/:owner/:repo', {owner, repo}).get();
          if (repository) {
            repository.core = _.omit(
              repository.core, 'id', 'connection', 'connector', 'reviewableBadge', 'errorCode',
              'error', 'hookEvents'
            );
            repository = _.omit(repository, 'adminUserKeys', 'current', 'issues', 'protection');
            reviewKeys = reviewKeys.concat(
              _.values(repository.pullRequests), _.values(repository.oldPullRequests));
            yield writeItem(
              NodeFire.escape(repo), mapAllUserKeys(repository, `/repositories/${owner}/${repo}`));
          }
          pace.op();
        });
      });
    });
  });
}

function *extractReviews() {
  if (!reviewKeys.length) return;
  yield writeCollection('reviews', function*() {
    yield eachLimit(reviewKeys, 25, function*(reviewKey) {
      const review = yield db.child('reviews/:reviewKey', {reviewKey}).get();
      review.core = _.omit(review.core, 'lastSweepTimestamp');
      review.discussions = _.pick(review.discussions, discussion => {
        discussion.comments =
          _.pick(discussion.comments, (comment, commentKey) => !/^gh-/.test(commentKey));
        return !_.isEmpty(discussion.comments);
      });
      if (_.isEmpty(review.discussions)) delete review.discussions;
      _.each(review.tracker, tracker => {
        tracker.participants = _.omit(tracker.participants, (participant, userKey) => {
          return !userMap[userKey] && participant.role === 'mentioned';
        });
      });
      delete review.gitHubComments;
      review.sentiments = _.pick(review.sentiments, sentiment => {
        sentiment.comments =
          _.pick(sentiment.comments, (comment, commentKey) => !/^gh-/.test(commentKey));
        return !_.isEmpty(sentiment.comments);
      });
      if (_.isEmpty(review.sentiments)) delete review.sentiments;
      yield writeItem(reviewKey, mapAllUserKeys(review, `/reviews/${reviewKey}`));
      pace.op();
    });
  });
}

function *extractLinemaps() {
  if (!reviewKeys.length) return;
  yield writeCollection('linemaps', function*() {
    yield eachLimit(reviewKeys, 25, function*(reviewKey) {
      const linemap = yield db.child('linemaps/:reviewKey', {reviewKey}).get();
      if (linemap) yield writeItem(reviewKey, linemap);
      pace.op();
    });
  });
}

function *extractFilemaps() {
  if (!reviewKeys.length) return;
  yield writeCollection('filemaps', function*() {
    yield eachLimit(reviewKeys, 25, function*(reviewKey) {
      const filemap = yield db.child('filemaps/:reviewKey', {reviewKey}).get();
      if (filemap) yield writeItem(reviewKey, filemap);
      pace.op();
    });
  });
}

function *extractUsers() {
  if (_.isEmpty(userMap)) return;
  yield writeCollection('users', function*() {
    yield eachOfLimit(userMap, 25, function*(newUserKey, oldUserKey) {
      let user = yield db.child('users/:oldUserKey', {oldUserKey}).get();
      user = _.omit(user, 'stripe', 'enrollments', 'index', 'notifications');
      delete user.core;
      if (user.settings) delete user.settings.dismissals;
      if (user.state) user.state = _.pick(user.state, reviewKeys);
      yield writeItem(newUserKey, mapAllUserKeys(user, `/users/${oldUserKey}`));
      pace.op();
    });
  });
}

let newCollection = true;

function *writeCollection(key, writer) {
  if (!writer) {
    writer = key;
    key = null;
  }
  if (!writer.name && key) Object.defineProperty(writer, 'name', {value: `writeCollection_${key}`});
  yield out.write(`${newCollection ? '' : ', '}${key ? JSON.stringify(key) + ': ' : ''}{`);
  newCollection = true;
  yield writer();
  yield out.write('}');
  newCollection = false;
}

function *writeItem(key, value) {
  const prefix = newCollection ? '' : ', ';
  newCollection = false;
  yield out.write(`${prefix}${JSON.stringify(key)}: ${JSON.stringify(value)}`);
}

function mapAllUserKeys(object, context) {
  if (_.isString(object)) {
    if (/^github:\d+$/.test(object)) return mapUserKey(object, context);
    if (/^github:\d+(\s*,\s*github:\d+)*$/.test(object)) {
      return _(object).split(/\s*,\s*/).map(mapUserKey).uniq().join(',');
    }
  } else if (_.isObject(object)) {
    for (const key in object) {
      const value = mapAllUserKeys(object[key], context + '/' + key);
      const newKey = mapAllUserKeys(key, context + '/$key');
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

function mapUserKey(userKey, context) {
  const newUserKey = userMap[userKey] || args.ghost;
  if (!newUserKey) throw new Error(`User not mapped and no ghost specified: ${userKey}`);
  if (newUserKey === args.ghost) ghostedUsers.push({userKey, context});
  return newUserKey;
}
