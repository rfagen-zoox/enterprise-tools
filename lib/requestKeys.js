const _ = require('lodash');
const request = require('request');
const HttpsAgent = require('agentkeepalive').HttpsAgent;
const ms = require('ms');
const requireEnvVars = require('./requireEnvVars.js');

requireEnvVars('REVIEWABLE_FIREBASE', 'REVIEWABLE_FIREBASE_AUTH');

const firebaseUrl = 'https://' + process.env.REVIEWABLE_FIREBASE + '.firebaseio.com';
const firebaseAuth = process.env.REVIEWABLE_FIREBASE_AUTH;

const agent = new HttpsAgent({
  keepAliveMsecs: ms('1s'), keepAliveTimeout: ms('15s'), timeout: ms('30s'), maxSockets: 3,
  maxFreeSockets: 1
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
              `Request for ${uriPath} returned ${response.statusCode}: ${response.body}`);
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

module.exports = requestKeys;
