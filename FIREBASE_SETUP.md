# Firebase Credentials Setup

This project requires Firebase credentials to run. The credentials file should **NEVER** be committed to git.

## For Local Development

1. Obtain your Firebase service account credentials JSON file
2. Save it as `eco-guardian-bd74f-firebase-adminsdk-thlcj-b60714ed55.json` in the project root
3. The scraper will automatically use this file when running locally

## For GitHub Actions

The workflow uses the `FIREBASE_KEY` secret instead of a file.

**Setup:**
1. Go to your repository on GitHub
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `FIREBASE_KEY`
5. Value: Paste the entire contents of your Firebase JSON credentials file
6. Click **Add secret**

The scraper will automatically detect and use the environment variable when running in GitHub Actions.
