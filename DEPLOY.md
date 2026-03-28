# IMBA Beacon Deployment

## Current Stack

- Frontend + backend served from the same Node app
- Entry point: `server.js`
- Waitlist storage: `data/waitlist.json`

## Important Note

The current backend stores waitlist submissions in a local JSON file.

This is fine for local development and quick demos, but on most cloud hosts this file storage is **not persistent** across rebuilds, restarts, or redeploys.

For a real production deployment, the next step should be moving waitlist storage to:

- SQLite
- PostgreSQL
- MongoDB

## Render Deployment

This project already includes `render.yaml`.

### Steps

1. Push this project to GitHub.
2. Log in to Render.
3. Create a new Web Service from the GitHub repo.
4. Render should detect:
   - Build Command: `npm install`
   - Start Command: `npm start`
5. Deploy.

### Result

Your site will be available at a Render URL like:

`https://imba-beacon.onrender.com`

## Local Run

```bash
npm install
npm start
```

Then open:

`http://127.0.0.1:3000`
