# Remote setup

This local repo is prepared, but remote access requires GitHub authentication.

## Option A: HTTPS token
1. Create a GitHub PAT with `repo` scope.
2. Run:
   - `git remote add origin https://github.com/limtaewon/trading.git`
   - `git push -u origin main`
3. Enter username + PAT when prompted.

## Option B: SSH key
1. Generate key: `ssh-keygen -t ed25519 -C "your_email"`
2. Add public key to GitHub SSH keys.
3. Run:
   - `git remote add origin git@github.com:limtaewon/trading.git`
   - `git push -u origin main`
