"""
etl/extractors/organic_extractor.py
Extracts organic content performance from Instagram, TikTok, Reddit.
Schema differences vs paid: no spend, no campaign hierarchy, no direct attribution.
"""
import hashlib, json, logging, random, time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Iterator
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import DATA_RAW_DIR, PIPELINE_VERSION
log = logging.getLogger(__name__)

TOPICS = ["route_share","race_recap","training_tip","club_spotlight",
          "challenge_completion","gear_review","athlete_story","product_feature"]

class OrganicExtractor:
    """
    Extracts organic content from Instagram Graph API, TikTok Display API, Reddit.
    
    Schema differences captured per platform:
      Instagram: avg_watch_time in MILLISECONDS, reach+impressions both available,
                 follows = per-post follower delta, website_clicks = outbound
      TikTok:    average_time_watched in SECONDS, play_count=view_count,
                 collect_count=saves, fans_delta=follower delta, engagement_rate native
      Reddit:    score=net_upvotes (not raw), upvote_ratio (not like_rate),
                 unique_views requires mod access, no video watch-time metric
    """
    def __init__(self, ig_token="your_ig_token", ig_acct="your_ig_acct",
                 tt_token="your_tt_token", rd_token="your_rd_token"):
        self._ig_token = ig_token; self._ig_acct = ig_acct
        self._tt_token = tt_token; self._rd_token = rd_token
        self._is_simulated = any(t.startswith("your_") for t in [ig_token,tt_token,rd_token])
        log.info(f"OrganicExtractor: {'simulated' if self._is_simulated else 'live'}")

    def extract_instagram(self, start_date, end_date):
        rows = self._simulate_instagram(start_date, end_date) if self._is_simulated else []
        log.info(f"Instagram organic: {len(rows):,} posts"); return rows

    def extract_tiktok(self, start_date, end_date):
        rows = self._simulate_tiktok(start_date, end_date) if self._is_simulated else []
        log.info(f"TikTok organic: {len(rows):,} posts"); return rows

    def extract_reddit(self, start_date, end_date):
        rows = self._simulate_reddit(start_date, end_date) if self._is_simulated else []
        log.info(f"Reddit organic: {len(rows):,} posts"); return rows

    def save_raw(self, ig, tt, rd, start_date):
        out_dir = DATA_RAW_DIR / "organic" / start_date.strftime("%Y-%m-%d")
        out_dir.mkdir(parents=True, exist_ok=True)
        for name, rows in [("instagram",ig),("tiktok",tt),("reddit",rd)]:
            p = out_dir / f"{name}_organic_raw.json"
            with open(p,"w") as f:
                json.dump({"_meta":{"source":f"{name}_organic_api","simulated":self._is_simulated,
                           "pipeline_version":PIPELINE_VERSION,"row_count":len(rows),
                           "channel_type":"organic"},"data":rows}, f, indent=2)
            log.info(f"  Saved {name}: {len(rows):,} rows")

    def _simulate_instagram(self, start_date, end_date):
        random.seed(int(hashlib.md5(f"ig_{start_date}".encode()).hexdigest(),16)%99991)
        MTYPES = [("REELS",0.40),("IMAGE",0.25),("CAROUSEL_ALBUM",0.20),("STORY",0.15)]
        rows = []; n = 5000
        for day in _dr(start_date, end_date):
            if random.random() > 0.43: continue
            mtype = random.choices([m[0] for m in MTYPES],[m[1] for m in MTYPES])[0]
            is_viral = random.random() < 0.08
            reach = int(_j(55000 if is_viral else 9500))
            impr  = int(reach * _j(1.32))
            likes = int(reach * _j(0.06 if mtype=="REELS" else 0.03))
            saves = int(reach * _j(0.04 if mtype=="REELS" else 0.02))
            comms = int(likes * _j(0.09)); shares = int(reach * _j(0.03))
            vv    = int(impr * _j(0.62)) if mtype in ("REELS","STORY") else 0
            rows.append({
                "media_id":f"IG_{n}","id":f"17841400{n}","media_type":mtype,
                "timestamp":f"{day}T{random.randint(6,20):02d}:00:00+0000",
                "topic":random.choice(TOPICS),"is_viral":is_viral,
                # Instagram-specific field names
                "reach":reach,"impressions":impr,"like_count":likes,
                "comments_count":comms,"saved":saves,"shares":shares,
                "video_views":vv,
                "avg_watch_time_ms":int(_j(14500)) if mtype=="REELS" else 0,  # MILLISECONDS
                "website_clicks":int(reach*_j(0.008)),"follows":int(_j(14)),
                "profile_visits":int(reach*_j(0.04)),
                "total_interactions":likes+comms+saves+shares,
            }); n+=1
        return rows

    def _simulate_tiktok(self, start_date, end_date):
        random.seed(int(hashlib.md5(f"tt_{start_date}".encode()).hexdigest(),16)%99991)
        VTYPES=[("Standard Video",0.55),("Duet",0.15),("Stitch",0.15),("Live",0.15)]
        rows=[]; n=7000
        for day in _dr(start_date, end_date):
            if random.random() > 0.43: continue
            vtype = random.choices([v[0] for v in VTYPES],[v[1] for v in VTYPES])[0]
            is_viral = random.random() < 0.08
            plays = int(_j(72000 if is_viral else 11000))
            likes = int(plays*_j(0.072)); comms=int(plays*_j(0.008))
            shares=int(plays*_j(0.018)); collects=int(plays*_j(0.012))
            rows.append({
                "id":f"TT_{n}","video_id":f"720{n}",
                "create_time":int(datetime(day.year,day.month,day.day,random.randint(6,20)).timestamp()),
                "content_type":vtype,"topic":random.choice(TOPICS),"is_viral":is_viral,
                # TikTok-specific field names
                "view_count":plays,"play_count":plays,          # TikTok uses play_count
                "like_count":likes,"comment_count":comms,
                "share_count":shares,"collect_count":collects,  # collect = saves/bookmarks
                "average_time_watched":round(_j(9.2),1),         # SECONDS (not ms like IG)
                "full_video_watched_rate":round(_j(0.31),4),     # completion rate 0-1
                "engagement_rate":round((likes+comms+shares)/max(plays,1),5),  # native field
                "reach":int(plays*_j(0.72)),
                "fans_delta":int(_j(32)),                        # follower delta (not "follows")
                "profile_visits":int(plays*_j(0.038)),
                "duet_count":int(_j(6)) if vtype=="Duet" else 0,
                "stitch_count":int(_j(4)) if vtype=="Stitch" else 0,
            }); n+=1
        return rows

    def _simulate_reddit(self, start_date, end_date):
        random.seed(int(hashlib.md5(f"rd_{start_date}".encode()).hexdigest(),16)%99991)
        SUBS=["r/running","r/cycling","r/fitness","r/triathlon"]
        PTYPES=[("Text Post",0.45),("Image Post",0.30),("Link Post",0.15),("Video Post",0.10)]
        rows=[]; n=3000
        for day in _dr(start_date, end_date):
            if random.random() > 0.38: continue
            ptype=random.choices([p[0] for p in PTYPES],[p[1] for p in PTYPES])[0]
            is_viral=random.random()<0.06
            score=int(_j(1800 if is_viral else 280))
            rows.append({
                "id":f"t3_{n}","post_id":f"RD_{n}",        # Reddit post ID format
                "subreddit":random.choice(SUBS),
                "created_utc":int(datetime(day.year,day.month,day.day,random.randint(8,22)).timestamp()),
                "post_type":ptype,"topic":random.choice(TOPICS),"is_viral":is_viral,
                # Reddit-specific field names
                "score":score,                               # net upvotes (not "likes")
                "upvote_ratio":round(_j(0.88,pct=0.08),3), # (not "like_rate")
                "num_comments":int(score*_j(0.18)),
                "num_shares":int(score*_j(0.05)),
                "unique_views":int(score*_j(3.8)),          # mod access only
                "outbound_clicks":int(score*_j(0.09)),
                "is_video":ptype=="Video Post",
                "video_views":int(score*_j(3.0)) if ptype=="Video Post" else 0,
                "subscriber_delta":int(_j(3)) if is_viral else 0,
            }); n+=1
        return rows

def _dr(s,e):
    d=s
    while d<=e: yield d; d+=timedelta(days=1)
def _j(b,pct=0.22): return max(0.0,b*random.uniform(1-pct,1+pct))

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO,format="%(levelname)s  %(message)s")
    ext=OrganicExtractor(); s=date(2024,10,1); e=date(2025,3,31)
    ig=ext.extract_instagram(s,e); tt=ext.extract_tiktok(s,e); rd=ext.extract_reddit(s,e)
    ext.save_raw(ig,tt,rd,s)
    print(f"IG:{len(ig)} TT:{len(tt)} RD:{len(rd)}")