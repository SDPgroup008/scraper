# Setting up GitHub Actions for the Scraper

## Required Secret

You need to add the Firebase credentials as a GitHub repository secret:

1. Go to your repository on GitHub
2. Click **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Set the name as: `FIREBASE_KEY`
5. For the value, copy the **entire contents** of your `eco-guardian-bd74f-firebase-adminsdk-thlcj-b60714ed55.json` file
6. Click **Add secret**

## Workflow Details

The scraper runs automatically:
- **Every 6 hours** (00:00, 06:00, 12:00, 18:00 UTC)
- Can be **manually triggered** from the Actions tab

## Manual Trigger

To run the scraper manually:
1. Go to the **Actions** tab in your repository
2. Select **Scrape and Upload Events** from the left sidebar
3. Click **Run workflow** button
4. Click the green **Run workflow** button to confirm

## Monitoring

- Check the **Actions** tab to see workflow runs
- If a run fails, logs will be uploaded as artifacts
- You can download the logs from the failed run's page
