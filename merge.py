#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 订阅源合并与健康检测（并发版）
"""
import json, os, time, random, hashlib, configparser
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SOURCES_FILE = os.path.join(BASE_DIR, "sources.txt")
CONFIG_FILE = os.path.join(BASE_DIR, "config.ini")
OUTPUT_FILE = os.path.join(BASE_DIR, "merged.json")
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

def load_sources():
    """读取 sources.txt"""
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
        "live_sample_channels": s.getint("live_sample_channels", 3),
        "http_timeout": s.getint("http_timeout", 8),
        "concurrency": s.getint("concurrency", 15),
        "wallpaper": s.get("wallpaper", ""),
    }

def fetch_source(item):
    name, url = item["name"], item["url"]
    ok, code, text = http_get(url, timeout=15)
    if not ok:
        return name, None, f"HTTP {code}"
    try:
        return name, json.loads(text), None
    except Exception as e:
        return name, None, f"JSON解析失败: {e}"

def merge_sites(sites_list):
    merged, order = {}, []
    for src in sites_list:
        for site in src:
            key = site.get("key", "")
            if key and key not in merged:
                merged[key] = site
                order.append(key)
    return [merged[k] for k in order]

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
    """检测一个点播站：api 可达 + ext jar 可达"""
    issues = []
    api = site.get("api", "")
    if api:
        ok, code, _ = http_head(api, timeout=timeout)
        if not ok:
            issues.append(f"api不可达({code})")
    ext = site.get("ext", "")
    if ext and isinstance(ext, str) and ext.startswith("http"):
        ok, code, _ = http_head(ext, timeout=timeout)
        if not ok:
            issues.append(f"ext/jar不可达({code})")
    return len(issues) == 0, issues

def parse_live_urls(live_url):
    ok, code, text = http_get(live_url, timeout=10)
    if not ok:
        return []
    urls = []
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "://" in line:
            if "," in line and not line.startswith("http"):
                urls.append(line.split(",", 1)[1].strip())
            elif line.startswith("http"):
                urls.append(line)
    return urls

def check_live(live, sample=3, timeout=6):
    live_url = live.get("url", "")
    if not live_url:
        return False, ["无url"]
    streams = parse_live_urls(live_url)
    if not streams:
        return False, ["无法解析频道列表"]
    n = min(sample, len(streams))
    samples = random.sample(streams, n)
    ok_count = 0
    issues = []
    for u in samples:
        ok, code, _ = http_head(u, timeout=timeout)
        if ok:
            ok_count += 1
        else:
            issues.append(f"流不可达({code})")
    passed = ok_count >= (n // 2 + 1)
    return passed, [f"抽样{ok_count}/{n}通过"] + issues

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
    live_sample = settings["live_sample_channels"]
    timeout = settings["http_timeout"]
    workers = settings["concurrency"]

    # 1. 并发拉取所有源
    print("并发拉取源...")
    all_sites, all_lives, all_parses, jar_candidates = [], [], [], []
    errors = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(fetch_source, s): s for s in remote_sources}
        for fut in as_completed(futs):
            name, data, err = fut.result()
            if err:
                errors.append(f"{name}: {err}")
                print(f"  X {name}: {err}")
                continue
            all_sites.append(data.get("sites", []))
            all_lives.append(data.get("lives", []))
            all_parses.append(data.get("parses", []))
            if data.get("spider"):
                jar_candidates.append(data["spider"])
            print(f"  OK {name}")

    for ls in extra_lives:
        all_lives.append([ls])

    # 2. 合并
    sites = merge_sites(all_sites)
    lives = merge_named(all_lives)
    parses = merge_named(all_parses)
    best_jar = pick_best_jar(jar_candidates)
    print(f"合并后: 站点{len(sites)} 直播{len(lives)} 解析{len(parses)}")

    # 3. 并发展点播检测
    print("并发展点播检测...")
    new_state = {"sites": {}, "lives": {}, "last_good": state.get("last_good")}
    site_results = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(check_site, s, timeout): s for s in sites}
        for fut in as_completed(futs):
            site = futs[fut]
            ok, issues = fut.result()
            site_results[site.get("key", "")] = (ok, issues)

    good_sites, failed_sites = [], []
    for site in sites:
        key = site.get("key", "")
        ok, issues = site_results.get(key, (False, ["检测异常"]))
        prev = state["sites"].get(key, {"fail_count": 0})
        fc = 0 if ok else prev.get("fail_count", 0) + 1
        new_state["sites"][key] = {"fail_count": fc, "ok": ok, "issues": issues, "last_check": now_str()}
        if fc < max_fail:
            good_sites.append(site)
        else:
            failed_sites.append((site, fc, issues))

    # 4. 并发测直播
    print("并发展直播检测...")
    live_results = {}
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {ex.submit(check_live, lv, live_sample, min(timeout, 6)): lv for lv in lives}
        for fut in as_completed(futs):
            live = futs[fut]
            ok, issues = fut.result()
            sid = hashlib.md5(f"{live.get('name','')}|{live.get('url','')}".encode()).hexdigest()
            live_results[sid] = (ok, issues)

    good_lives, failed_lives = [], []
    for live in lives:
        name = live.get("name", live.get("url", ""))
        sid = hashlib.md5(f"{name}|{live.get('url','')}".encode()).hexdigest()
        ok, issues = live_results.get(sid, (False, ["检测异常"]))
        prev = state["lives"].get(sid, {"fail_count": 0})
        fc = 0 if ok else prev.get("fail_count", 0) + 1
        new_state["lives"][sid] = {"fail_count": fc, "ok": ok, "issues": issues, "last_check": now_str()}
        if fc < max_fail:
            good_lives.append(live)
        else:
            failed_lives.append((live, fc, issues))

    # 5. 缓存兜底
    if len(good_sites) == 0 and state.get("last_good"):
        print("!! 全部站点不可用，回退到上次版本")
        merged = state["last_good"]
    else:
        merged = {
            "spider": best_jar,
            "sites": good_sites,
            "lives": good_lives,
            "parses": parses,
            "wallpaper": settings["wallpaper"],
            "update_time": now_str(),
            "version": "2.0"
        }
        new_state["last_good"] = merged

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(merged, f, ensure_ascii=False, indent=2)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(new_state, f, ensure_ascii=False, indent=2)

    # 健康报告
    with open(REPORT_FILE, "w", encoding="utf-8") as f:
        f.write(f"# TVBox 订阅源健康报告\n\n**检测时间**: {now_str()}\n\n")
        f.write(f"**可用站点**: {len(good_sites)} / {len(sites)}\n\n")
        f.write(f"**可用直播**: {len(good_lives)} / {len(lives)}\n\n")
        f.write(f"**spider jar**: `{best_jar}`\n\n")
        f.write(f"**总耗时**: {int(time.time()-t_start)} 秒\n\n")
        if errors:
            f.write("## 拉取失败的源\n\n")
            for e in errors: f.write(f"- {e}\n")
        if failed_sites:
            f.write("\n## 已剔除或容忍中的站点\n\n")
            for site, fc, issues in failed_sites:
                f.write(f"- **{site.get('name','?')}** (key={site.get('key','?')}) 连续失败={fc}\n")
                for i in issues: f.write(f"  - {i}\n")
        if failed_lives:
            f.write("\n## 已剔除或容忍中的直播\n\n")
            for live, fc, issues in failed_lives:
                f.write(f"- **{live.get('name','?')}** 连续失败={fc}\n")
                for i in issues: f.write(f"  - {i}\n")

    print(f"=== 完成: 站点{len(good_sites)} 直播{len(good_lives)} 耗时{int(time.time()-t_start)}秒 ===")

if __name__ == "__main__":
    main()
