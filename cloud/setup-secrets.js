/**
 * Setup Script: Extract Google auth tokens for GitHub Secrets
 *
 * This reads your local ~/.clasprc.json and prints the values
 * you need to set as GitHub repository secrets.
 *
 * Usage: node setup-secrets.js
 */

const fs = require('fs');
const path = require('path');
const os = require('os');

const claspPath = path.join(os.homedir(), '.clasprc.json');

console.log('=== GitHub Secrets Setup Helper ===\n');

if (!fs.existsSync(claspPath)) {
  console.error(`ERROR: ${claspPath} not found.`);
  console.error('Make sure you have clasp configured with: npx @google/clasp login');
  process.exit(1);
}

try {
  const creds = JSON.parse(fs.readFileSync(claspPath, 'utf8'));
  const tok = creds.tokens.default;

  if (!tok.client_id || !tok.client_secret || !tok.refresh_token) {
    console.error('ERROR: Missing required token fields in .clasprc.json');
    console.error('Available fields:', Object.keys(tok).join(', '));
    process.exit(1);
  }

  console.log('Found auth tokens in ~/.clasprc.json\n');
  console.log('You need to set these 3 secrets in your GitHub repository.');
  console.log('Go to: https://github.com/YOUR_USERNAME/flipkart-tracker/settings/secrets/actions\n');
  console.log('─'.repeat(60));

  console.log('\nSecret Name: GOOGLE_CLIENT_ID');
  console.log(`Value: ${tok.client_id}`);

  console.log('\nSecret Name: GOOGLE_CLIENT_SECRET');
  console.log(`Value: ${tok.client_secret}`);

  console.log('\nSecret Name: GOOGLE_REFRESH_TOKEN');
  console.log(`Value: ${tok.refresh_token}`);

  console.log('\n' + '─'.repeat(60));
  console.log('\nOR use the GitHub CLI (gh) to set them automatically:');
  console.log('(Run these commands from the repo directory)\n');
  console.log(`gh secret set GOOGLE_CLIENT_ID --body "${tok.client_id}"`);
  console.log(`gh secret set GOOGLE_CLIENT_SECRET --body "${tok.client_secret}"`);
  console.log(`gh secret set GOOGLE_REFRESH_TOKEN --body "${tok.refresh_token}"`);

  console.log('\n' + '─'.repeat(60));
  console.log('\nIMPORTANT SECURITY NOTE:');
  console.log('These tokens give access to your Google account.');
  console.log('If using a PUBLIC repo, NEVER commit these values to code.');
  console.log('GitHub Secrets are encrypted and safe to use.\n');

} catch (e) {
  console.error('ERROR parsing .clasprc.json:', e.message);
  process.exit(1);
}
