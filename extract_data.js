#!/usr/bin/env node
'use strict';

const _ = require('lodash');
const commandLineArgs = require('command-line-args');
const fs = require('fs');
const forEachLimit = require('async/eachLimit');
const forEachOfLimit = require('async/eachOfLimit');
const forEachOf = require('async/eachOf');
const getUsage = require('command-line-usage');
const NodeFire = require('nodefire');
const PromiseWritable = require('promise-writable');

const commandLineOptions = [
  {name: 'repos', alias: 'r', typeLabel: '[underline]{repos.json}',
    description: 'A file with a JSON array of "owner/repo" repo names to extract.'},
  {name: 'users', alias: 'u', typeLabel: '[underline]{users.json}',
    description: 'A file with a JSON object of {"github:MMMM": "github:NNNN"} user id mappings.'},
  {name: 'output', alias: 'o', typeLabel: '[underline]{data.json}',
    description: 'Output JSON file for extracted data.'},
  {name: 'help', alias: 'h', type: Boolean,
    description: 'Display these usage instructions.'}
];

const usageSpec = [
  {header: 'Data extraction tool',
    content:
      'Extracts all data related to a set of repos from a Reviewable datastore, in preparation ' +
      'for transforming and loading it into another datastore.'
  },
  {header: 'Options', optionList: commandLineOptions}
];

const args = commandLineArgs(commandLineOptions);
if (args.help) {
  console.log(getUsage(usageSpec));
  process.exit(0);
}
for (const property of ['repos', 'users', 'output']) {
  if (!(property in args)) {
    console.log('Missing required option: ' + property + '.');
    process.exit(1);
  }
}

const userMap = JSON.parse(fs.readFileSync(args.users));
const repoNames =
  _(args.repos).thru(fs.readFileSync).thru(JSON.parse).map(_.toLower).uniq().value();
const groupedRepoNames = _.groupBy(repoNames, name => name.split('/')[0]);
const orgNames = _(repoNames).map(name => name.replace(/\/.*/, '')).uniq().value();

const out = new PromiseWritable(fs.createWriteStream(args.output));
out.stream.setMaxListeners(Infinity);

const pace = require('pace')(1 + orgNames.length + repoNames.length + _.size(userMap));

let reviewKeys = [];
let ghostedUsers = [];

async function extract() {
  await out.write('{\n');
  await extractOrganizations();
  await extractRepositories();
  reviewKeys = _.uniq(reviewKeys);
  pace.total += 3 * reviewKeys.length;
  await extractUsers();
  await extractReviews();
  await extractLinemaps();
  await extractFilemaps();
  await out.write('}\n');
  await out.end();
  pace.op();

  if (ghostedUsers.length) {
    ghostedUsers = _.uniqBy(ghostedUsers, 'userKey');
    console.log(`${ghostedUsers.length} users could not be mapped over:`);
    const users = await forEachLimit(ghostedUsers, 5, async item => {
      const user = await db.child('users/:userKey/core/public', {userKey: item.userKey}).get();
      return {
        username: user ? user.username : ` user ${item.userKey.replace(/github:/, '')}`,
        context: item.context
      };
    });
    console.log(
      _(users)
        .sortBy(user => _.toLower(user.username))
        .map(user => `${user.username} @ ${user.context}`)
        .join('\n')
    );
  }
}

extract().then(() => {
  process.exit(0);
}, e => {
  console.log();
  if (e.errors) console.log(e.errors);
  console.log(e.stack);
  process.exit(1);
});


async function extractOrganizations() {
  if (!orgNames.length) return;
  await writeCollection('organizations', async () => {
    await forEachLimit(orgNames, 5, async org => {
      const organization = await db.child('organizations/:org', {org}).get();
      if (organization) {
        await writeItem(
          NodeFire.escape(org), mapAllUserKeys(organization, `/organizations/${org}`));
      }
      pace.op();
    });
  }, true);
}

async function extractRepositories() {
  if (!repoNames.length) return;
  await writeCollection('repositories', async () => {
    await forEachOf(groupedRepoNames, async (repoNamesGroup, orgName) => {
      await writeCollection(NodeFire.escape(orgName), async () => {
        await forEachLimit(repoNamesGroup, 10, async repoName => {
          const [owner, repo] = repoName.split('/');
          let repository = await db.child('repositories/:owner/:repo', {owner, repo}).get();
          if (repository) {
            repository.core = _.omit(
              repository.core, 'id', 'connection', 'connector', 'reviewableBadge', 'errorCode',
              'error', 'hookEvents'
            );
            repository = _.omit(repository, 'adminUserKeys', 'current', 'issues', 'protection');
            reviewKeys = reviewKeys.concat(
              _.values(repository.pullRequests), _.values(repository.oldPullRequests));
            await writeItem(
              NodeFire.escape(repo), mapAllUserKeys(repository, `/repositories/${owner}/${repo}`));
          }
          pace.op();
        });
      });
    });
  }, true);
}

async function extractReviews() {
  if (!reviewKeys.length) return;
  await forEachLimit(reviewKeys, 25, async reviewKey => {
    const review = await db.child('reviews/:reviewKey', {reviewKey}).get();
    review.core = _.omit(review.core, 'lastSweepTimestamp');
    review.discussions = _.pickBy(review.discussions, discussion => {
      discussion.comments =
        _.pickBy(discussion.comments, (comment, commentKey) => !/^gh-/.test(commentKey));
      return !_.isEmpty(discussion.comments);
    });
    if (_.isEmpty(review.discussions)) delete review.discussions;
    _.forEach(review.tracker, tracker => {
      tracker.participants = _.omitBy(tracker.participants, (participant, userKey) => {
        return !userMap[userKey] && participant.role === 'mentioned';
      });
    });
    delete review.gitHubComments;
    review.sentiments = _.pickBy(review.sentiments, sentiment => {
      sentiment.comments =
        _.pickBy(sentiment.comments, (comment, commentKey) => !/^gh-/.test(commentKey));
      return !_.isEmpty(sentiment.comments);
    });
    if (_.isEmpty(review.sentiments)) delete review.sentiments;
    await writeItem(`reviews/${reviewKey}`, mapAllUserKeys(review, `/reviews/${reviewKey}`), true);
    pace.op();
  });
}

async function extractLinemaps() {
  if (!reviewKeys.length) return;
  await writeCollection('linemaps', async () => {
    await forEachLimit(reviewKeys, 25, async reviewKey => {
      const linemap = await db.child('linemaps/:reviewKey', {reviewKey}).get();
      if (linemap) await writeItem(reviewKey, linemap);
      pace.op();
    });
  }, true);
}

async function extractFilemaps() {
  if (!reviewKeys.length) return;
  await writeCollection('filemaps', async () => {
    await forEachLimit(reviewKeys, 25, async reviewKey => {
      const filemap = await db.child('filemaps/:reviewKey', {reviewKey}).get();
      if (filemap) await writeItem(reviewKey, filemap);
      pace.op();
    });
  }, true);
}

async function extractUsers() {
  if (_.isEmpty(userMap)) return;
  await writeCollection('users', async () => {
    await forEachOfLimit(userMap, 25, async (newUserKey, oldUserKey) => {
      let user = await db.child('users/:oldUserKey', {oldUserKey}).get();
      user = _.omit(user, 'stripe', 'enrollments', 'index', 'notifications');
      delete user.core;
      if (user.settings) delete user.settings.dismissals;
      if (user.state) user.state = _.pick(user.state, reviewKeys);
      await writeItem(newUserKey, mapAllUserKeys(user, `/users/${oldUserKey}`));
      pace.op();
    });
  }, true);
}

let newCollection = true;

async function writeCollection(key, writer, top) {
  if (!writer.name) Object.defineProperty(writer, 'name', {value: `writeCollection_${key}`});
  await out.write(`${newCollection ? '' : ', '}${key ? JSON.stringify(key) + ': ' : ''}{`);
  newCollection = true;
  await writer();
  await out.write(`}${top ? '\n' : ''}`);
  newCollection = false;
}

async function writeItem(key, value, top) {
  const prefix = newCollection ? '' : ', ';
  newCollection = false;
  await out.write(`${prefix}${JSON.stringify(key)}: ${JSON.stringify(value)}${top ? '\n' : ''}`);
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
  const newUserKey = userMap[userKey] || 'github:1';
  if (!newUserKey) throw new Error(`User not mapped and no ghost specified: ${userKey}`);
  if (newUserKey === 'github:1') ghostedUsers.push({userKey, context});
  return newUserKey;
}
