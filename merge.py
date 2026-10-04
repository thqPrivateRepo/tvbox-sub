#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 订阅源合并与健康检测 v3.9
- 支持多仓嵌套格式（{urls:[{url,name}]}），自动过滤 // 注释行
- 点播：多源全部保留不去重、并发检测死链、缓存兜底
- 报告：按订阅源分组列出站点明细，标注每个站来源和剔除原因
- 直播：只保留 CCTV1-17 + CCTV5+ + CCTV4欧洲/美洲 + CCTV4K/8K + 卫视，多线路合并
- 直播检测：全部线路检测，全挂频道剔除，死线路单独剔除
- 排序：CCTV 按数字升序，卫视按拼音 A-Z
- 站点名敏感词替换：含"访问/网站/获取/接口/公众号/starlink/更多"的改名为"小乌龟"
"""
import json, os, re, time, random, hashlib, configparser, sys, subprocess
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_FILE = os.path.join(BASE_DIR, "sources.txt")
CONFIG_FILE = os.path.join(BASE_DIR, "config.ini")
OUTPUT_FILE = os.path.join(BASE_DIR, "merged.json")
LIVE_FILE = os.path.join(BASE_DIR, "live.txt")
STATE_FILE = os.path.join(BASE_DIR, "health_state.json")
REPORT_FILE = os.path.join(BASE_DIR, "health_report.md")
BEIJING_TZ = timezone(timedelta(hours=8))
UA = {"User-Agent": "okhttp/4.9.3"}

def now_str():
    return datetime.now(BEIJING_TZ).strftime("%Y-%m-%d %H:%M:%S")

def http_head(url, timeout=8):
    try:
        t0 = time.time()
        r = requests.head(url, timeout=timeout, allow_redirects=True, headers=UA)
        return (r.status_code < 400, r.status_code, int((time.time()-t0)*1000))
    except Exception as e:
        return (False, str(e)[:80], 0)

def http_get(url, timeout=15):
    try:
        r = requests.get(url, timeout=timeout, headers=UA)
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
    """宽容解析 JSON：自动去掉 // 注释行、BOM、首尾空白"""
    text = text.lstrip("\ufeff").strip()
    lines = text.splitlines()
    cleaned = []
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("//"):
            continue
        cleaned.append(line)
    return json.loads("\n".join(cleaned))

# ============ 站点名敏感词替换 ============
BLOCK_WORDS = ["访问", "网站", "获取", "接口", "公众号", "starlink", "更多"]

def sanitize_site_name(name):
    if any(w in name.lower() for w in BLOCK_WORDS):
        return "小乌龟"
    return name

# ============ 频道名归一化 ============
ALLOWED_CCTV = {f"CCTV{i}" for i in range(1, 18)} | {
    "CCTV5+", "CCTV4欧洲", "CCTV4美洲",
    "CCTV4K超高清", "CCTV8K超高清"
}

def normalize_cctv(name):
    name = name.strip().rstrip("-").strip()
    m = re.match(r'^CCTV[-_]?0*(\d+\+?)(?:HD)?$', name, re.IGNORECASE)
    if m:
        return f"CCTV{m.group(1)}"
    m = re.match(r'^CCTV[-_]?0*(\d+\+?)(?:HD)?[一-龥].*$', name, re.IGNORECASE)
    if m:
        num = m.group(1)
        rest = name[m.end(1):].replace("HD","").strip()
        rest = re.sub(r'^[-_]?', '', rest)
        if rest in ("欧洲", "美洲"):
            return f"CCTV{num}{rest}"
        return f"CCTV{num}"
    m = re.match(r'^CCTV[-_]?(\d+)K(?:超高清)?$', name, re.IGNORECASE)
    if m:
        return f"CCTV{m.group(1)}K超高清"
    m = re.match(r'^CCTV[-_](.+)$', name)
    if m:
        return f"CCTV{m.group(1)}"
    return name

def normalize_weishi(name):
    name = name.strip().rstrip("-").strip()
    name = re.sub(r'(HD|4K|4k)$', '', name, flags=re.IGNORECASE).strip()
    name = re.sub(r'\(.*?\)', '', name).strip()
    return name

def normalize_channel(name):
    n = name.upper()
    if n.startswith("CCTV"):
        return normalize_cctv(name)
    return normalize_weishi(name)

def is_wanted_channel(name):
    n = name.upper()
    if n.startswith("CCTV"):
        return name in ALLOWED_CCTV
    if "卫视" in name:
        return True
    return False

# ============ 直播解析（支持 txt + m3u）============
def parse_live_text(text):
    channels = {}
    lines = text.splitlines()
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        if line.startswith("#EXTINF"):
            if "," in line:
                ch_name = line.split(",", 1)[1].strip()
            else:
                i += 1; continue
            i += 1
            while i < len(lines):
                url_line = lines[i].strip()
                if url_line.startswith("http"):
                    channels.setdefault(ch_name, [])
                    if url_line not in channels[ch_name]:
                        channels[ch_name].append(url_line)
                    i += 1; break
                i += 1
            continue
        if not line or line.startswith("#") or "#genre#" in line:
            i += 1; continue
        if "," in line:
            parts = line.split(",", 1)
            ch_name = parts[0].strip()
            ch_url = parts[1].strip()
            if ch_url.startswith("http"):
                channels.setdefault(ch_name, [])
                if ch_url not in channels[ch_name]:
                    channels[ch_name].append(ch_url)
        i += 1
    return channels

def process_live_sources(live_urls):
    try:
        from pypinyin import lazy_pinyin
    except ImportError:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "pypinyin", "-q"])
        from pypinyin import lazy_pinyin

    def cctv_sort_key(name):
        m = re.match(r'^CCTV(\d+)(.*)$', name)
        if m:
            num = int(m.group(1))
            rest = m.group(2)
            if rest == "+":
                return (num, 0, rest)
            if rest in ("欧洲", "美洲"):
                return (num, 1, rest)
            if "K" in rest:
                return (100, 0, rest)
            return (num, 0, rest)
        return (999, 0, name)

    def weishi_sort_key(name):
        return "".join(lazy_pinyin(name))

    all_channels = {}
    for live_url in live_urls:
        ok, code, text = http_get(live_url, timeout=15)
        if not ok:
            print(f"  直播源拉取失败: {live_url[:60]} ({code})")
            continue
        channels = parse_live_text(text)
        print(f"  直播源 {live_url[:60]}: {len(channels)} 个频道")
        for ch_name, urls in channels.items():
            norm = normalize_channel(ch_name)
            if not is_wanted_channel(norm):
                continue
            all_channels.setdefault(norm, [])
            for u in urls:
                if u not in all_channels[norm]:
                    all_channels[norm].append(u)
    cctv = {k:v for k,v in all_channels.items() if k.upper().startswith("CCTV")}
    weishi = {k:v for k,v in all_channels.items() if "卫视" in k and not k.upper().startswith("CCTV")}

    total_before = len(cctv) + len(weishi)
    total_lines = sum(len(v) for v in cctv.values()) + sum(len(v) for v in weishi.values())
    print(f"  检测直播线路可用性（{total_before}个频道/{total_lines}条线路）...")
    check_urls = []
    for ch_name, urls in {**cctv, **weishi}.items():
        for url in urls:
            check_urls.append((ch_name, url))

    channel_ok = {}
    dead_lines = set()
    with ThreadPoolExecutor(max_workers=15) as ex:
        futs = {ex.submit(check_live_url, url): (ch, url) for ch, url in check_urls}
        for fut in as_completed(futs):
            ch_name, url = futs[fut]
            if fut.result():
                channel_ok[ch_name] = True
            else:
                dead_lines.add((ch_name, url))

    cctv = {k:v for k,v in cctv.items() if k in channel_ok}
    weishi = {k:v for k,v in weishi.items() if k in channel_ok}
    cctv = {k:[u for u in v if (k,u) not in dead_lines] for k,v in cctv.items()}
    weishi = {k:[u for u in v if (k,u) not in dead_lines] for k,v in weishi.items()}
    removed = total_before - len(channel_ok)
    dead_count = len(dead_lines)
    print(f"  直播检测完成: {len(channel_ok)}个频道可用, 剔除{removed}个无可用频道, 剔除{dead_count}条死线路")

    lines = []
    if cctv:
        lines.append("央视频道,#genre#")
        for ch in sorted(cctv.keys(), key=cctv_sort_key):
            for u in cctv[ch]:
                lines.append(f"{ch},{u}")
        lines.append("")
    if weishi:
        lines.append("卫视频道,#genre#")
        for ch in sorted(weishi.keys(), key=weishi_sort_key):
            for u in weishi[ch]:
                lines.append(f"{ch},{u}")
    with open(LIVE_FILE, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    tc = len(cctv) + len(weishi)
    tl = sum(len(v) for v in cctv.values()) + sum(len(v) for v in weishi.values())
    print(f"  直播合并完成: {tc} 个频道, {tl} 条线路")
    return tc, tl

# ============ 点播源处理 ============
def load_sources():
    remote, lives = [], []
    with open(SOURCES_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            typ, name, url = parts[0].strip(), parts[1].strip(), parts[2].strip()
            if typ == "点播":
                remote.append({"name": name, "url": url})
            elif typ == "直播":
                lives.append({"name": name, "url": url, "type": 0})
    return remote, lives

def load_config():
    cp = configparser.ConfigParser()
    cp.read(CONFIG_FILE, encoding="utf-8")
    s = cp["settings"] if "settings" in cp else {}
    return {
        "max_fail_count": s.getint("max_fail_count", 2),
        "http_timeout": s.getint("http_timeout", 8),
        "concurrency": s.getint("concurrency", 15),
        "wallpaper": s.get("wallpaper", ""),
        "live_cos_url": s.get("live_cos_url", ""),
    }

def fetch_source_recursive(source_item, depth=0):
    """拉取点播源，支持多仓嵌套 + // 注释行"""
    name, url = source_item["name"], source_item["url"]
    ok, code, text = http_get(url, timeout=15)
    if not ok:
        return name, None, f"HTTP {code}", []
    try:
        data = parse_json_lenient(text)
    except Exception as e:
        return name, None, f"JSON解析失败: {e}", []

    # 多仓格式：{urls: [{url, name}]}
    if isinstance(data, dict) and "urls" in data and isinstance(data["urls"], list):
        if depth >= 2:
            return name, None, "多仓嵌套太深", []
        all_sites, all_parses = [], []
        sub_jars = []
        site_origins = []
        for sub in data["urls"]:
            sub_url = sub.get("url", "")
            sub_name = sub.get("name", sub_url[:20])
            if not sub_url:
                continue
            sub_name2, sub_data, sub_err, _ = fetch_source_recursive({"name": sub_name, "url": sub_url}, depth+1)
            if sub_err:
                print(f"    X {sub_name}: {sub_err}")
                continue
            if sub_data:
                sub_sites = sub_data.get("sites", [])
                all_sites.extend(sub_sites)
                all_parses.extend(sub_data.get("parses", []))
                if sub_data.get("spider"):
                    sub_jars.append(sub_data["spider"])
                for s in sub_sites:
                    site_origins.append((s, sub_name))
                print(f"    OK {sub_name} ({len(sub_sites)}站)")
        return name, {"sites": all_sites, "parses": all_parses, "spider": None, "_sub_jars": sub_jars}, None, site_origins

    # 标准 TVBox JSON
    site_origins = [(s, name) for s in data.get("sites", [])]
    return name, data, None, site_origins

def merge_sites(sites_list):
    result = []
    for src in sites_list:
        for site in src:
            result.append(site)
    return result

def merge_named(items_list):
    merged, order = {}, []
    for src in items_list:
        for it in src:
            sid = hashlib.md5(f"{it.get('name','')}|{it.get('url','')}|{it.get('api','')}".encode()).hexdigest()
            if sid not in merged:
                merged[sid] = it
                order.append(sid)
    return [merged[k] for k in order]

def pick_best_jar(jar_list):
    from collections import Counter
    if not jar_list:
        return ""
    return Counter(jar_list).most_common(1)[0][0]

def check_site(site, timeout=8):
    issues = []
    api = site.get("api", "")
    if api and (api.startswith("http://") or api.startswith("https://")):
        ok, code, _ = http_head(api, timeout=timeout)
        if not ok:
            if code == 405:
                ok2, code2, _ = http_get(api, timeout=timeout)
                if not ok2:
                    issues.append(f"api不可达({code2})")
            else:
                issues.append(f"api不可达({code})")
    ext = site.get("ext", "")
    if ext and isinstance(ext, str) and ext.startswith(("http://", "https://")):
        ok, code, _ = http_head(ext, timeout=timeout)
        if not ok:
            issues.append(f"ext/jar不可达({code})")
    return len(issues) == 0, issues

def main():
    t_start = time.time()
    print(f"=== TVBox 源合并开始 {now_str()} ===")
    settings = load_config()
    remote_sources, extra_lives = load_sources()
    print(f"配置: 点播源{len(remote_sources)}个, 直播线路{len(extra_lives)}条")

    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            state = json.load(f)
    else:
        state = {"sites": {}, "lives": {}, "last_good": None}

    max_fail = settings["max_fail_count"]
    timeout = settings["http_timeout"]
    workers = settings["concurrency"]

    print("并发拉取点播源...")
    all_sites = []
    all_parses = []
    jar_candidates = []
    errors = []
    site_origin = {}
    source_detail = {}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_source_recursive, s): s for s in remote_sources}
        for fut in as_completed(futs):
            top_name, data, err, origins = fut.result()
            if err:
                errors.append(f"{top_name}: {err}")
                print(f"  X {top_name}: {err}")
                continue
            sites = data.get("sites", [])
            all_sites.append(sites)
            all_parses.append(data.get("parses", []))
            if data.get("spider"):
                jar_candidates.append(data["spider"])
            for j in data.get("_sub_jars", []):
                jar_candidates.append(j)
            source_detail[top_name] = {}
            for s, origin in origins:
                site_origin[id(s)] = origin
                source_detail[top_name].setdefault(origin, []).append(s.get("name", "?"))
            print(f"  OK {top_name}: {len(sites)}站")

    sites = merge_sites(all_sites)

    for site in sites:
        old = site.get("name", "")
        site["name"] = sanitize_site_name(old)

    parses = merge_named(all_parses)
    best_jar = pick_best_jar(jar_candidates)

    print("处理直播源（过滤央视卫视+合并多线路）...")
    live_urls = [lv["url"] for lv in extra_lives]
    live_count, live_line_count = process_live_sources(live_urls)

    print("并发展开检测...")
    new_state = {"sites": {}, "lives": {}, "last_good": state.get("last_good")}
    site_results = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(check_site, s, timeout): s for s in sites}
        for fut in as_completed(futs):
            site = futs[fut]
            ok, issues = fut.result()
            site_results[id(site)] = (ok, issues)

    good_sites = []
    site_status = {}
    for idx, site in enumerate(sites):
        ok, issues = site_results.get(id(site), (False, ["检测异常"]))
        sk = f"{site.get('key','')}_{idx}"
        prev = state["sites"].get(sk, {"fail_count": 0})
        fc = 0 if ok else prev.get("fail_count", 0) + 1
        new_state["sites"][sk] = {"fail_count": fc, "ok": ok, "issues": issues, "last_check": now_str()}
        if fc < max_fail:
            good_sites.append(site)
            site_status[id(site)] = (True, "")
        else:
            site_status[id(site)] = (False, "; ".join(issues) if issues else f"连续失败{fc}次")

    live_cos_url = settings.get("live_cos_url", "")
    lives_field = [{"name": "央视+卫视", "type": 0, "url": live_cos_url}] if live_cos_url else extra_lives

    print(f"合并后: 站点{len(good_sites)}/{len(sites)}, 直播{live_count}个频道/{live_line_count}条线路")

    if len(good_sites) == 0 and state.get("last_good"):
        print("!! 全部站点不可用，回退到上次版本")
        merged = state["last_good"]
    else:
        merged = {
            "spider": best_jar,
            "sites": good_sites,
            "lives": lives_field,
            "parses": parses,
            "wallpaper": settings["wallpaper"],
            "update_time": now_str(),
            "version": "3.9"
        }
        new_state["last_good"] = merged

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(new_state, f, ensure_ascii=False, indent=2)

    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(f"# TVBox 订阅源健康报告\n\n**检测时间**: {now_str()}\n\n")
        f.write(f"**可用站点**: {len(good_sites)} / {len(sites)}\n\n")
        f.write(f"**直播**: {live_count} 个频道, {live_line_count} 条线路\n\n")
        f.write(f"**spider jar**: `{best_jar}`\n\n")
        f.write(f"**总耗时**: {int(time.time()-t_start)} 秒\n\n")

        if errors:
            f.write("## 拉取失败的点播源\n\n")
            for e in errors:
                f.write(f"- {e}\n")

        f.write("\n## 站点明细（按订阅源分组）\n\n")
        top_to_sites = {}
        for site in sites:
            sid = id(site)
            origin = site_origin.get(sid, "未知")
            top_name = "未知"
            for tn, subs in source_detail.items():
                if origin in subs:
                    top_name = tn
                    break
            top_to_sites.setdefault(top_name, {}).setdefault(origin, []).append(
                (site.get("name", "?"), site_status.get(sid, (True, "")))
            )

        for top_name, subs in top_to_sites.items():
            f.write(f"### 订阅源：{top_name}\n\n")
            for origin, site_list in subs.items():
                ok_count = sum(1 for _, (ok, _) in site_list if ok)
                bad_count = len(site_list) - ok_count
                f.write(f"**{origin}**（共{len(site_list)}站，可用{ok_count}，剔除{bad_count}）\n\n")
                for sname, (ok, reason) in site_list:
                    if ok:
                        f.write(f"- {sname} ✅\n")
                    else:
                        f.write(f"- {sname} ❌ 剔除：{reason}\n")
                f.write("\n")

    print(f"=== 完成: 站点{len(good_sites)} 直播{live_count}频道 耗时{int(time.time()-t_start)}秒 ===")

if __name__ == "__main__":
    main()
