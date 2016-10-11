#!/usr/bin/env node
'use strict';

const _ = require('lodash');
const co = require('co');
const constants = require('constants');
const crypto = require('crypto');
const eachLimit = require('async-co/eachLimit');
const forge = require('node-forge');
const HttpsAgent = require('agentkeepalive').HttpsAgent;
const ms = require('ms');
const NodeFire = require('nodefire');
const request = require('request');

NodeFire.setCacheSize(0);

const agent = new HttpsAgent({
  keepAliveMsecs: ms('1s'), keepAliveTimeout: ms('15s'), timeout: ms('30s'), maxSockets: 3,
  maxFreeSockets: 1
});

const requiredEnvVars = [
  'REVIEWABLE_FIREBASE', 'REVIEWABLE_FIREBASE_AUTH', 'REVIEWABLE_ENCRYPTION_PRIVATE_KEYS'
];

for (let property of requiredEnvVars) {
  if (!process.env[property]) {
    console.log('Missing required environment variable: ' + property);
    process.exit(1);
  }
}

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
let updatedTokens = 0;

co(function*() {
  const results = yield [requestKeys('users'), db.auth(firebaseAuth)];
  const userKeys = results[0];
  pace.total = userKeys.length;
  yield eachLimit(userKeys, 100, function*(userKey) {
    yield db.child('users/:userKey/core/gitHubToken', {userKey}).transaction(
      oldToken => oldToken && recrypt(userKey, oldToken));
    pace.op();
  });
  console.log('Re-encrypted', updatedTokens, 'token' + (updatedTokens === 1 ? '' : 's'));
}).then(() => {
  process.exit(0);
}, e => {
  console.log();
  if (e.errors) console.log(e.errors);
  console.log(e.stack);
  process.exit(1);
});

function requestKeys(path) {
  return new Promise((resolve, reject) => {
    let tries = 0;
    const uriPath =
      '/' + _(path.split('/')).compact().map(part => encodeURIComponent(part)).join('/');
    const req = () => request(
      {
        uri: firebaseUrl + uriPath + '.json', agent: agent, qs: {auth: firebaseAuth, shallow: true}
      },
      (error, response, data) => {
        if (!error) {
          if (response.statusCode === 200) {
            try {
              resolve(_(JSON.parse(data)).keys().map(decode).value());
              return;
            } catch (e) {
              error = e;
            }
          } else {
            error = new Error(
              'Request for ' + uriPath + ' returned ' + response.statusCode + ': ' + response.body);
          }
        }
        if (++tries <= 3) req(); else reject(error);
      }
    );
    req();
  });
}

function decode(string) {
  return string.replace(/\\../g, function(match) {
    return String.fromCharCode(parseInt(match.slice(1), 16));
  });
}

function recrypt(userKey, encrypted) {
  if (!encrypted) return;
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
    if (!plainText) {
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
