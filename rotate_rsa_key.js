#!/usr/bin/env node

global.Promise = require('bluebird');
Promise.co = require('co');
const _ = require('lodash');
const constants = require('constants');
const crypto = require('crypto');
const eachLimit = require('async-co/eachLimit');
const forge = require('node-forge');
const NodeFire = require('nodefire');
const requestKeys = require('./lib/requestKeys.js');
const requireEnvVars = require('./lib/requireEnvVars.js');

NodeFire.setCacheSize(0);

requireEnvVars(
  'REVIEWABLE_FIREBASE', 'REVIEWABLE_FIREBASE_AUTH', 'REVIEWABLE_ENCRYPTION_PRIVATE_KEYS'
);

const rsaKeys = parsePrivateKeys();
if (!rsaKeys.length) {
  console.log('No private keys specified in REVIEWABLE_ENCRYPTION_PRIVATE_KEYS');
  process.exit(1);
}

const rsa2Keys = createNodeForgeKeys(rsaKeys);
const publicKey = extractPublicKey();
const rsa2Options = {md: forge.md.sha256.create()};

const firebaseUrl = 'https://' + process.env.REVIEWABLE_FIREBASE + '.firebaseio.com';
const firebaseAuth = process.env.REVIEWABLE_FIREBASE_AUTH;
const db = new NodeFire(firebaseUrl);

const pace = require('pace')(1);
let updatedTokens = 0, scannedUsers = 0, checkedTokens = 0;

Promise.co(function*() {
  const results = yield [requestKeys('users'), db.auth(firebaseAuth)];
  const userKeys = results[0];
  pace.total = userKeys.length;
  yield eachLimit(userKeys, 200, function*(userKey) {
    yield db.child('users/:userKey/core/gitHubToken', {userKey}).transaction(
      oldToken => recrypt(userKey, oldToken)
    );
    pace.op();
  });
  console.log(
    `Re-encrypted ${pluralize(updatedTokens, 'token')}`,
    `(checked ${pluralize(checkedTokens, 'tokens')} from ${pluralize(scannedUsers, 'users')})`);
}).then(() => {
  process.exit(0);
}, e => {
  console.log();
  if (e.errors) console.log(e.errors);
  console.log(e.stack);
  process.exit(1);
});

function recrypt(userKey, encrypted) {
  scannedUsers += 1;
  if (!encrypted) return;
  checkedTokens += 1;
  let plainText = encrypted;
  if (/^rsa/.test(encrypted)) {
    let version;
    const cipherText = new Buffer(encrypted.replace(/^rsa\d*:/, matched => {
      version = matched.slice(0, -1);
      return '';
    }), 'base64');
    let keys, decrypt;
    switch (version) {
      case 'rsa':
        keys = rsaKeys;
        decrypt = (key, text) => crypto.privateDecrypt(key, text).toString('utf8');
        break;
      case 'rsa2':
        // Node's openssl bindings hardcode SHA-1 for RSA crypto, so use node-forge instead.
        keys = rsa2Keys;
        decrypt = (key, text) => key.decrypt(text, 'RSA-OAEP', rsa2Options);
        break;
      default:
        throw new Error('Unknown encryption prefix: ' + version);
    }
    const errors = [];
    for (let i = 0; i < keys.length; i++) {
      try {
        plainText = decrypt(keys[i], cipherText);
        if (/[0-9a-f]+/i.test(plainText)) {
          // No need to recrypt if already using primary key with latest crypto version.
          if (i === 0 && version === 'rsa2') return;
          updatedTokens++;
          break;
        }
      } catch (e) {
        errors.push(e);
      }
    }
    if (plainText === encrypted) {
      const e = new Error('Unable to decrypt token with any key: ' + encrypted);
      e.errors = errors;
      throw e;
    }
  }
  return 'rsa2:' + forge.util.encode64(publicKey.encrypt(plainText, 'RSA-OAEP', rsa2Options));
}

function normalizePrivateKey(pkcsKey) {
  return pkcsKey.replace(
    /-----BEGIN (.*?) KEY-----((.|\n|\r)*?)-----END (\1) KEY-----/,
    function(match, keyType, contents) {
      return '-----BEGIN ' + keyType + ' KEY-----\n' +
        contents.replace(/\s+/g, '').replace(/.{64}/g, '$&\n').replace(/\n*$/, '\n') +
        '-----END ' + keyType + ' KEY-----\n';
    }
  );
}

function parsePrivateKeys() {
  if (!process.env.REVIEWABLE_ENCRYPTION_PRIVATE_KEYS) return [];
  return _.map(process.env.REVIEWABLE_ENCRYPTION_PRIVATE_KEYS.split(','), function(key) {
    return {padding: constants.RSA_PKCS1_PADDING, key: normalizePrivateKey(key)};
  });
}

function createNodeForgeKeys(nodeKeys) {
  return _.map(nodeKeys, keyDef => forge.pki.privateKeyFromPem(keyDef.key));
}

function extractPublicKey() {
  if (!(rsa2Keys && rsa2Keys.length)) return;
  const privateKey = rsa2Keys[0];
  return forge.pki.setRsaPublicKey(privateKey.n, privateKey.e);
}

function pluralize(count, item) {
  return count + ' ' + item + (count === 1 ? '' : 's');
}
