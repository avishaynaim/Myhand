# PostgreSQL Setup for Railway - Complete Guide

## Why PostgreSQL?

Railway uses **ephemeral storage** - the filesystem resets on every deployment. This means your SQLite database (`yad2_monitor.db`) gets deleted on each deploy, losing all apartment data, user preferences, and favorites.

**PostgreSQL solves this** by providing persistent database storage that survives deployments.

## ✅ You're Already Ready!

The code has been updated to **automatically detect and use PostgreSQL** when available. You just need to add it in Railway.

## Quick Setup (2 minutes)

### Step 1: Add PostgreSQL Database

1. **Go to your Railway project**
   - Open https://railway.app/dashboard
   - Click on your project

2. **Add PostgreSQL**
   - Click **"New"** button (top right)
   - Select **"Database"**
   - Choose **"PostgreSQL"**
   - Railway will provision the database automatically

3. **Wait for deployment**
   - Railway will deploy PostgreSQL (takes ~30 seconds)
   - A `DATABASE_URL` environment variable will be automatically added to your worker service

### Step 2: Redeploy Worker Service

1. **Go to your worker service**
   - Click on the **"worker"** service in your project

2. **Trigger redeploy**
   - Go to **"Deployments"** tab
   - Click **"Redeploy"** on the latest deployment
   - OR just push a new commit to GitHub (auto-deploys)

### Step 3: Verify It's Working

Check the logs for this message:

```
📊 Detected DATABASE_URL - using PostgreSQL
🐘 Initializing PostgreSQL database
✅ PostgreSQL tables initialized successfully
```

If you see this, you're all set! 🎉

## How It Works

The code automatically chooses the database:

```python
# db_wrapper.py detects DATABASE_URL
if os.environ.get('DATABASE_URL'):
    # Use PostgreSQL
    return PostgreSQLDatabase(DATABASE_URL)
else:
    # Fallback to SQLite
    return Database('yad2_monitor.db')
```

**Railway automatically sets DATABASE_URL** when you add PostgreSQL, so no manual configuration needed!

## What Gets Migrated

All data structures work identically:

- ✅ Apartments (listings, prices, details)
- ✅ Price history (all historical price changes)
- ✅ Telegram users (multi-user support)
- ✅ User favorites (per-user favorites)
- ✅ User filters (per-user search filters)
- ✅ User preferences (notification settings)
- ✅ Search URLs (monitored searches)
- ✅ Daily summaries (stats by date)
- ✅ Scrape logs (monitoring activity)

## Database Connection

Railway provides:
- **DATABASE_URL**: Full PostgreSQL connection string
- **Automatic SSL**: Secure connections
- **Connection pooling**: Handled automatically
- **Backups**: Automatic daily backups

## Troubleshooting

### "DATABASE_URL not found"

**Symptom**: Logs show "📊 Using SQLite database"

**Solution**:
1. Verify PostgreSQL database is running in Railway
2. Check if DATABASE_URL exists in worker service variables
3. If missing, go to PostgreSQL service → Variables → Copy DATABASE_URL
4. Add it manually to worker service → Variables → Add `DATABASE_URL`

### "Connection refused" or "could not connect to server"

**Symptom**: Logs show psycopg2 connection errors

**Solution**:
1. Verify PostgreSQL service is running (should show green checkmark)
2. Check DATABASE_URL format: `postgresql://user:pass@host:port/db`
3. Restart PostgreSQL service if needed
4. Redeploy worker service

### "relation does not exist"

**Symptom**: SQL errors about missing tables

**Solution**:
1. Tables are created automatically on first startup
2. Check logs for "✅ PostgreSQL tables initialized successfully"
3. If missing, there may be a connection issue
4. Check DATABASE_URL permissions

### Starting fresh

To reset the database:
1. Delete the PostgreSQL service in Railway
2. Create a new PostgreSQL database
3. Redeploy worker service

All tables will be recreated automatically.

## Migration from Existing SQLite Data

If you have existing SQLite data you want to migrate:

### Option 1: Manual Migration (Recommended)

1. Add PostgreSQL in Railway (follows steps above)
2. Your old SQLite data is in ephemeral storage (lost on deploy)
3. Start fresh with PostgreSQL - it will build up new data as it scrapes

### Option 2: Export/Import (Advanced)

If you need to preserve existing data:

1. **Before adding PostgreSQL**, download your SQLite database:
   ```bash
   railway run python -c "import database; db = database.Database(); db.backup('backup.db')"
   railway download backup.db
   ```

2. **Add PostgreSQL in Railway**

3. **Create import script** (local):
   ```python
   # import_to_postgres.py
   import sqlite3
   import psycopg2
   import os

   # Connect to SQLite backup
   sqlite_conn = sqlite3.connect('backup.db')
   sqlite_cursor = sqlite_conn.cursor()

   # Connect to PostgreSQL (set DATABASE_URL environment variable)
   pg_conn = psycopg2.connect(os.environ['DATABASE_URL'])
   pg_cursor = pg_conn.cursor()

   # Copy apartments
   sqlite_cursor.execute('SELECT * FROM apartments')
   for row in sqlite_cursor.fetchall():
       pg_cursor.execute('INSERT INTO apartments VALUES (%s, ...)', row)

   pg_conn.commit()
   ```

4. **Run locally with Railway DATABASE_URL**

This is complex - usually not worth it. Fresh start recommended.

## Costs

- **Free Tier**: PostgreSQL included in free tier
- **Paid Plans**: $5/month includes PostgreSQL
- **Storage**: 1GB included, expandable
- **Backups**: Automatic, no extra cost

## Benefits Over SQLite

| Feature | SQLite (Ephemeral) | PostgreSQL (Railway) |
|---------|-------------------|---------------------|
| Data persistence | ❌ Lost on deploy | ✅ Permanent |
| Backups | ❌ Manual only | ✅ Automatic daily |
| Concurrent access | ⚠️ Limited | ✅ Full support |
| Scalability | ❌ Single file | ✅ Scalable |
| Production-ready | ❌ No | ✅ Yes |

## Next Steps

After PostgreSQL is set up:

1. ✅ Data will persist across deployments
2. ✅ All user data will be saved
3. ✅ Price history will accumulate
4. ✅ Multi-user support fully functional

## Support

If you encounter issues:

1. Check Railway logs: `railway logs`
2. Verify DATABASE_URL: `railway variables`
3. Check PostgreSQL service status in Railway dashboard
4. Railway Discord: https://discord.gg/railway

## Summary

**Required action**: Just click "New" → "Database" → "PostgreSQL" in Railway dashboard.

That's it! The code handles everything else automatically. 🚀
