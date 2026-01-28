# Railway Persistent Storage Setup

## Problem

Railway uses ephemeral storage - the filesystem resets on every deployment. This means your SQLite database (`yad2_monitor.db`) gets deleted on each deploy, losing all apartment data.

## Solution: Railway Volumes

Railway provides **Volumes** for persistent storage that survives deployments.

## Setup Instructions

### Method 1: Railway Dashboard (Recommended)

1. **Go to your Railway project**
   - https://railway.app/dashboard
   - Click on your `worker` service

2. **Add a Volume**
   - Click on the **"Variables"** tab
   - Scroll down to **"Volumes"** section
   - Click **"New Volume"**

3. **Configure the Volume**
   - **Mount Path:** `/data`
   - **Size:** 1 GB (more than enough for the database)
   - Click **"Add"**

4. **Redeploy**
   - Railway will automatically redeploy with the volume attached
   - The database will now persist at `/data/yad2_monitor.db`

### Method 2: Railway CLI

```bash
# Login to Railway
railway login

# Link to your project (if not already linked)
railway link

# Create a volume
railway volume create --name yad2-data --mount /data

# Redeploy
railway up
```

## How It Works

The code automatically detects the `/data` directory:

```python
# Priority 1: Check for /data directory (Railway volume)
if os.path.exists('/data'):
    db_path = '/data/yad2_monitor.db'

# Priority 2: Use current directory
else:
    db_path = 'yad2_monitor.db'
```

## Verify It's Working

After deployment, check the logs:

```
📁 Using persistent storage: /data/yad2_monitor.db
```

If you see this message, your database is now persistent! ✅

## Alternative: PostgreSQL

For production workloads, consider PostgreSQL instead of SQLite:

1. **Add PostgreSQL in Railway Dashboard**
   - New → Database → PostgreSQL
   - Railway will add `DATABASE_URL` automatically

2. **Update code to use PostgreSQL**
   - Install `psycopg2`: Add to `requirements.txt`
   - Modify `database.py` to use PostgreSQL when `DATABASE_URL` is set

## Cost

- **Volumes:** Included in Railway Pro plan ($5/month)
- **Free Tier:** Limited volume storage available

## Notes

- Volume data persists across deployments
- Volume data is backed up by Railway
- You can download volume data via CLI: `railway volume download`
- Each service can have multiple volumes
- Volumes are project-specific and not shared between services

## Troubleshooting

### Database still gets reset

Check logs to ensure volume is mounted:
```bash
railway logs | grep "Using persistent storage"
```

If you don't see this message, the volume might not be mounted correctly.

### Volume not available in free tier

If volumes aren't available, consider:
1. Upgrade to Railway Pro ($5/month)
2. Use PostgreSQL database (included in all tiers)
3. Use external database service (e.g., Supabase, PlanetScale)

## Summary

✅ **Before:** Database deleted on every deployment
✅ **After:** Database persists forever with `/data` volume

**Required Action:** Add a volume at `/data` in Railway dashboard (takes 1 minute)
