import crypto from 'crypto';
import * as constants from 'constants';
import _ from 'lodash';

const keys = [];

export async function fetchToken(userKey) {
  const encryptedToken = await db.child('users/:userKey/core/gitHubToken', {userKey}).get();
  if (!encryptedToken) throw new Error(`User ${userKey} not signed in to Reviewable`);
  if (!/^rsa\d*:/.test(encryptedToken)) return encryptedToken;
  const cipherText = Buffer.from(encryptedToken.replace(/^rsa\d:/, ''), 'base64');
  if (!process.env.REVIEWABLE_ENCRYPTION_PRIVATE_KEYS) {
    throw new Error(
      `Unable to decrypt token for user ${userKey} without REVIEWABLE_ENCRYPTION_PRIVATE_KEYS`);
  }
  for (const key of keys) {
    const token = crypto.privateDecrypt(key, cipherText).toString('utf8');
    if (/^[ -~]+$/.test(token)) return token;
  }
  throw new Error(`Unable to decrypt token for user ${userKey} with any private key`);
}


if (process.env.REVIEWABLE_ENCRYPTION_PRIVATE_KEYS) {
  _.forEach(process.env.REVIEWABLE_ENCRYPTION_PRIVATE_KEYS.split(','), pemKey => {
    const key = crypto.createPrivateKey(normalizePrivateKey(pemKey));
    keys.push({key, padding: constants.RSA_PKCS1_OAEP_PADDING, oaepHash: 'sha256'});
  });
}


function normalizePrivateKey(pkcsKey) {
  return pkcsKey.replace(
    /-----BEGIN (.*?) KEY-----([\s\S]*?)-----END (\1) KEY-----/,
    (match, keyType, contents) => {
      return '-----BEGIN ' + keyType + ' KEY-----\n' +
        contents.replace(/\\n|\s+/g, '').replace(/.{64}/g, '$&\n').replace(/\n*$/, '\n') +
        '-----END ' + keyType + ' KEY-----\n';
    }
  );
}
