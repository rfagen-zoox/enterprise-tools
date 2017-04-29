function requireEnvVars() {
  for (let property of arguments) {
    if (!process.env[property]) {
      console.log('Missing required environment variable: ' + property);
      process.exit(1);
    }
  }
}

module.exports = requireEnvVars;
