# Setup PostgreSQL on Railway (Free & Persistent)

## Why PostgreSQL?

- ✅ Included free with Railway
- ✅ Persistent storage (never deleted)
- ✅ Better for production
- ✅ Handles concurrent access better than SQLite

## Setup Steps

### 1. Add PostgreSQL to Your Project

In Railway Dashboard:

1. Go to your project
2. Click **"New"** button (top right)
3. Select **"Database"** → **"PostgreSQL"**
4. Railway will automatically create the database
5. Railway will add `DATABASE_URL` environment variable

### 2. Update requirements.txt

Add PostgreSQL adapter:

```txt
psycopg2-binary==2.9.9
```

### 3. Modify database.py to support PostgreSQL

The code will automatically detect `DATABASE_URL` and use PostgreSQL instead of SQLite.

## That's it!

Your database will now persist forever, even across deployments.

## Verify It Works

Check logs after deployment:
```
📁 Using PostgreSQL database
```

## Cost

**FREE** - PostgreSQL is included in Railway's free tier.
