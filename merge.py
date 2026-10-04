#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 订阅源合并 v4.2
- 点播：全部原样合并，不做死链检测
- 相对路径自动补全为完整 URL
- 支持多仓嵌套格式
- 直播：只保留央视+卫视，全量线路检测，全挂频道剔除
"""
import json, os, re, time, hashlib, configparser, sys, subprocess
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urljoin
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_FILE = os.path.join(BASE_DIR, "sources.txt")
CONFIG_FILE = os.path.join(BASE_DIR, "config.ini")
OUTPUT_FILE = os.path.join(BASE_DIR, "merged.json")
LIVE_FILE = os.path.join(BASE_DIR, "live.txt")
REPORT_FILE = os.path.join(BASE_DIR, "health_report.md")
BEIJING_TZ = timezone(timedelta(hours=8))
UA = {"User-Agent": "okhttp/4.9.3"}

def now_str():
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

def http_get(url, timeout=15):
    try:
        r = requests.get(url, timeout=timeout, headers=UA)
        r.encoding = "utf-8"
        return r.status_code < 400, r.status_code, r.text
    except Exception as e:
        return False, str(e)[:80], ""

def check_live_url(url, timeout=5):
    try:
        r = requests.get(url, timeout=timeout, stream=True, headers=UA)
        ok = r.status_code < 400
        r.close()
        return ok
    except Exception:
        return False

def parse_json_lenient(text):
    text = text.lstrip("\ufeff").strip()
    lines = text.splitlines()
    cleaned = [l for l in lines if not l.strip().startswith("//")]
    return json.loads("\n".join(cleaned))

def resolve_path(path, base_url):
    if not isinstance(path, str):
        return path
    if path.startswith(("./", "../")):
        return urljoin(base_url, path)
    return path

BLOCK_WORDS = ["访问", "网站", "获取", "接口", "公众号", "starlink", "更多"]
def sanitize_site_name(name):
    if any(w in name.lower() for w in BLOCK_WORDS):
        return "小乌龟"
    return name

ALLOWED_CCTV = {f"CCTV{i}" for i in range(1, 18)} | {
    "CCTV5+", "CCTV4欧洲", "CCTV4美洲",
    "CCTV4K超高清", "CCTV8K超高清"
}

def normalize_cctv(name):
    name = name.strip().rstrip("-").strip()
    m = re.match(r'^CCTV[-_]?0*(\d+\+?)(?:HD)?$', name, re.IGNORECASE)
    if m: return f"CCTV{m.group(1)}"
    m = re.match(r'^CCTV[-_]?0*(\d+\+?)(?:HD)?[一-龥].*$', name, re.IGNORECASE)
    if m:
        num = m.group(1)
        rest = name[m.end(1):].replace("HD","").strip().lstrip("-_")
        if rest in ("欧洲","美洲"): return f"CCTV{num}{rest}"
        return f"CCTV{num}"
    m = re.match(r'^CCTV[-_]?(\d+)K(?:超高清)?$', name, re.IGNORECASE)
    if m: return f"CCTV{m.group(1)}K超高清"
    m = re.match(r'^CCTV[-_](.+)$', name)
    if m: return f"CCTV{m.group(1)}"
    return name

def normalize_weishi(name):
    name = name.strip().rstrip("-").strip()
    name = re.sub(r'(HD|4K|4k)$', '', name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\(.*?\)', '', name).strip()
    return name

def normalize_channel(name):
    if name.upper().startswith("CCTV"):
        return normalize_cctv(name)
    return normalize_weishi(name)

def is_wanted_channel(name):
    if name.upper().startswith("CCTV"):
        return name in ALLOWED_CCTV
    return "卫视" in name

def parse_live_text(text):
    channels = {}
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF"):
            ch_name = line.split(",",1)[1].strip() if "," in line else ""
            i += 1
            while i < len(lines):
                u = lines[i].strip()
                if u.startswith("http"):
                    channels.setdefault(ch_name, [])
                    if u not in channels[ch_name]: channels[ch_name].append(u)
                    i += 1; break
                i += 1
            continue
        if not line or line.startswith("#") or "#genre#" in line:
            i += 1; continue
        if "," in line:
            n,u = line.split(",",1)
            n,u = n.strip(),u.strip()
            if u.startswith("http"):
                channels.setdefault(n, [])
                if u not in channels[n]: channels[n].append(u)
        i += 1
    return channels

def process_live_sources(live_urls):
    try:
        from pypinyin import lazy_pinyin
    except ImportError:
        subprocess.check_call([sys.executable,"-m","pip","install","pypinyin","-q"])
        from pypinyin import lazy_pinyin

    def cctv_sort_key(name):
        m = re.match(r'^CCTV(\d+)(.*)$', name)
        if m:
            num = int(m.group(1)); rest = m.group(2)
            if rest == "+": return (num,0,rest)
            if rest in ("欧洲","美洲"): return (num,1,rest)
            if "K" in rest: return (100,0,rest)
            return (num,0,rest)
        return (999,0,name)
    def ws_key(name):
        return "".join(lazy_pinyin(name))

    all_channels = {}
    for live_url in live_urls:
        ok, code, text = http_get(live_url, timeout=15)
        if not ok:
            print(f"  直播源失败: {live_url[:50]} ({code})"); continue
        channels = parse_live_text(text)
        print(f"  直播源 {live_url[:50]}: {len(channels)}频道")
        for cn, urls in channels.items():
            norm = normalize_channel(cn)
            if not is_wanted_channel(norm): continue
            all_channels.setdefault(norm, [])
            for u in urls:
                if u not in all_channels[norm]: all_channels[norm].append(u)

    cctv = {k:v for k,v in all_channels.items() if k.upper().startswith("CCTV")}
    ws = {k:v for k,v in all_channels.items() if "卫视" in k and not k.upper().startswith("CCTV")}

    tb = len(cctv)+len(ws)
    tl = sum(len(v) for v in cctv.values())+sum(len(v) for v in ws.values())
    print(f"  检测直播线路（{tb}频道/{tl}线路）...")
    check_urls = [(cn,u) for cn,urls in {**cctv,**ws}.items() for u in urls]
    ch_ok = {}; dead = set()
    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(check_live_url,u):(cn,u) for cn,u in check_urls}
        for f in as_completed(futs):
            cn,u = futs[f]
            if f.result(): ch_ok[cn]=True
            else: dead.add((cn,u))
    cctv = {k:v for k,v in cctv.items() if k in ch_ok}
    ws = {k:v for k,v in ws.items() if k in ch_ok}
    cctv = {k:[u for u in v if (k,u) not in dead] for k,v in cctv.items()}
    ws = {k:[u for u in v if (k,u) not in dead] for k,v in ws.items()}

    lines = []
    if cctv:
        lines.append("央视频道,#genre#")
        for ch in sorted(cctv.keys(), key=cctv_sort_key):
            for u in cctv[ch]: lines.append(f"{ch},{u}")
        lines.append("")
    if ws:
        lines.append("卫视频道,#genre#")
        for ch in sorted(ws.keys(), key=ws_key):
            for u in ws[ch]: lines.append(f"{ch},{u}")
    with open(LIVE_FILE,"w",encoding="utf-8") as f: f.write("\n".join(lines))
    tc = len(cctv)+len(ws); tln = sum(len(v) for v in cctv.values())+sum(len(v) for v in ws.values())
    print(f"  直播: {tc}频道/{tln}线路")
    return tc, tln

def load_sources():
    remote, lives = [], []
    with open(SOURCES_FILE,"r",encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"): continue
            parts = line.split("|",2)
            if len(parts)<3: continue
            typ,name,url = parts[0].strip(),parts[1].strip(),parts[2].strip()
            if typ=="点播": remote.append({"name":name,"url":url})
            elif typ=="直播": lives.append({"name":name,"url":url,"type":0})
    return remote, lives

def load_config():
    cp = configparser.ConfigParser()
    cp.read(CONFIG_FILE, encoding="utf-8")
    s = cp["settings"] if "settings" in cp else {}
    return {
        "concurrency": s.getint("concurrency",15),
        "wallpaper": s.get("wallpaper",""),
        "live_cos_url": s.get("live_cos_url",""),
    }

def fix_paths(site, base):
    for f in ("api","ext"):
        v = site.get(f,"")
        if isinstance(v,str) and v.startswith(("./","../")):
            site[f] = urljoin(base, v)
    return site

def fetch_recursive(item, depth=0):
    name, url = item["name"], item["url"]
    ok, code, text = http_get(url, timeout=15)
    if not ok: return name, None, f"HTTP {code}", []
    try:
        data = parse_json_lenient(text)
    except Exception as e:
        return name, None, f"JSON错误: {e}", []

    if isinstance(data,dict) and "urls" in data and isinstance(data["urls"],list):
        if depth>=2: return name,None,"嵌套太深",[]
        ss, pp, sj, so = [], [], [], []
        for sub in data["urls"]:
            su = sub.get("url",""); sn = sub.get("name",su[:20])
            if not su: continue
            _, sd, se, _ = fetch_recursive({"name":sn,"url":su}, depth+1)
            if se: print(f"    X {sn}: {se}"); continue
            if sd:
                subs = sd.get("sites",[])
                ss.extend(subs); pp.extend(sd.get("parses",[]))
                if sd.get("spider"): sj.append(sd["spider"])
                for s in subs: so.append((s,sn))
                print(f"    OK {sn} ({len(subs)}站)")
        return name, {"sites":ss,"parses":pp,"spider":None,"_jars":sj}, None, so

    sites = data.get("sites",[])
    for s in sites: fix_paths(s, url)
    if data.get("spider") and isinstance(data["spider"],str) and data["spider"].startswith(("./","../")):
        data["spider"] = urljoin(url, data["spider"])
    return name, data, None, [(s,name) for s in sites]

def pick_jar(jars):
    from collections import Counter
    real = [j for j in jars if ".jar" in j.lower()]
    return Counter(real).most_common(1)[0][0] if real else ""

def main():
    t0 = time.time()
    print(f"=== TVBox 合并 {now_str()} ===")
    cfg = load_config()
    remote, lives = load_sources()
    print(f"点播源{len(remote)}个, 直播{len(lives)}条")

    print("拉取点播源...")
    all_sites, all_parses, jars, errors = [], [], [], []
    with ThreadPoolExecutor(max_workers=cfg["concurrency"]) as ex:
        futs = {ex.submit(fetch_recursive,s):s for s in remote}
        for f in as_completed(futs):
            tn, data, err, origins = f.result()
            if err: errors.append(f"{tn}: {err}"); print(f"  X {tn}: {err}"); continue
            sites = data.get("sites",[])
            all_sites.append(sites); all_parses.append(data.get("parses",[]))
            if data.get("spider"): jars.append(data["spider"])
            for j in data.get("_jars",[]): jars.append(j)
            print(f"  OK {tn}: {len(sites)}站")

    sites = []
    for src in all_sites:
        sites.extend(src)
    for s in sites:
        s["name"] = sanitize_site_name(s.get("name",""))

    pmerged = {}; porder = []
    for src in all_parses:
        for it in src:
            sid = hashlib.md5(f"{it.get('name','')}|{it.get('url','')}|{it.get('api','')}".encode()).hexdigest()
            if sid not in pmerged: pmerged[sid]=it; porder.append(sid)
    parses = [pmerged[k] for k in porder]
    best_jar = pick_jar(jars)

    print("处理直播...")
    live_count, live_lines = process_live_sources([lv["url"] for lv in lives])

    live_cos = cfg.get("live_cos_url","")
    lives_field = [{"name":"央视+卫视","type":0,"url":live_cos}] if live_cos else lives

    merged = {
        "spider": best_jar,
        "sites": sites,
        "lives": lives_field,
        "parses": parses,
        "wallpaper": cfg["wallpaper"],
        "update_time": now_str(),
        "version": "4.2"
    }
    with open(OUTPUT_FILE,"w",encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)

    with open(REPORT_FILE,"w",encoding="utf-8") as f:
        f.write(f"# TVBox 订阅源报告\n\n**时间**: {now_str()}\n\n")
        f.write(f"**站点**: {len(sites)}\n\n")
        f.write(f"**直播**: {live_count}频道/{live_lines}线路\n\n")
        f.write(f"**spider**: `{best_jar}`\n\n")
        f.write(f"**耗时**: {int(time.time()-t0)}秒\n\n")
        if errors:
            f.write("## 拉取失败的源\n\n")
            for e in errors: f.write(f"- {e}\n")

    print(f"=== 完成: 站点{len(sites)} 直播{live_count}频道 耗时{int(time.time()-t0)}秒 ===")

if __name__ == "__main__":
    main()
