#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TVBox 订阅源合并 v4.9
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
HEALTH_JSON_FILE = os.path.join(BASE_DIR, "merge_health.json")
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

def probe_site(site):
    """对站点的 api/ext/jar 首个 URL 做轻量探活，返回(ok, error)。
    仅 HTTP GET、5 秒超时、status<400 视为健康；非 URL 或缺失视为未探活（ok=True, note=无URL）。"""
    def first_url(v):
        if not isinstance(v, str):
            return None
        u = v.split(";")[0].strip()
        return u if u.startswith(("http://", "https://")) else None

    for f in ("api", "ext", "jar"):
        u = first_url(site.get(f, ""))
        if not u:
            continue
        try:
            r = requests.get(u, timeout=5, headers=UA, stream=True)
            ok = r.status_code < 400
            err = "" if ok else f"HTTP {r.status_code}"
            r.close()
            return ok, err
        except Exception as e:
            return False, str(e)[:80]
    return True, ""

def strip_inline_comment(line):
    in_str = False
    escape = False
    for i, ch in enumerate(line):
        if escape:
            escape = False; continue
        if ch == "\\":
            escape = True; continue
        if ch == '"':
            in_str = not in_str; continue
        if not in_str and ch == "/" and i + 1 < len(line) and line[i+1] == "/":
            return line[:i].rstrip()
    return line

def parse_json_lenient(text):
    text = text.lstrip("\ufeff").strip()
    lines = text.splitlines()
    cleaned = []
    for l in lines:
        s = l.strip()
        if s.startswith("//"):
            continue
        cleaned.append(strip_inline_comment(l))
    return json.loads("\n".join(cleaned))

BLOCK_WORDS = ["访问", "网站", "获取", "接口", "公众号", "starlink", "更多"]
def sanitize_site_name(name):
    if not isinstance(name, str):
        return "小乌龟"
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
    if not isinstance(name, str):
        return ""
    if name.upper().startswith("CCTV"):
        return normalize_cctv(name)
    return normalize_weishi(name)

def is_wanted_channel(name):
    if not name: return False
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

def get_pinyin_sorter():
    """获取拼音排序函数，失败则降级为默认字符串排序"""
    try:
        from pypinyin import lazy_pinyin
        return lambda name: "".join(lazy_pinyin(name))
    except ImportError:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", "pypinyin", "-q"],
                                  timeout=60)
            from pypinyin import lazy_pinyin
            return lambda name: "".join(lazy_pinyin(name))
        except Exception:
            return lambda name: name

def process_live_sources(live_urls, concurrency=15):
    ws_sort = get_pinyin_sorter()

    def cctv_sort_key(name):
        m = re.match(r'^CCTV(\d+)(.*)$', name)
        if m:
            num = int(m.group(1)); rest = m.group(2)
            if rest == "+": return (num,0,rest)
            if rest in ("欧洲","美洲"): return (num,1,rest)
            if "K" in rest: return (100,0,rest)
            return (num,0,rest)
        return (999,0,name)

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
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
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
        for ch in sorted(ws.keys(), key=ws_sort):
            for u in ws[ch]: lines.append(f"{ch},{u}")
    with open(LIVE_FILE,"w",encoding="utf-8") as f: f.write("\n".join(lines))
    tc = len(cctv)+len(ws); tln = sum(len(v) for v in cctv.values())+sum(len(v) for v in ws.values())
    print(f"  直播: {tc}频道/{tln}线路")
    return tc, tln

def load_sources():
    remote, lives = [], []
    if not os.path.exists(SOURCES_FILE):
        print(f"  警告: {SOURCES_FILE} 不存在")
        return remote, lives
    try:
        with open(SOURCES_FILE,"r",encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                parts = line.split("|",2)
                if len(parts)<3: continue
                typ,name,url = parts[0].strip(),parts[1].strip(),parts[2].strip()
                if typ=="点播": remote.append({"name":name,"url":url})
                elif typ=="直播": lives.append({"name":name,"url":url,"type":0})
    except Exception as e:
        print(f"  读取sources.txt失败: {e}")
    return remote, lives

def load_config():
    cp = configparser.ConfigParser()
    try:
        cp.read(CONFIG_FILE, encoding="utf-8")
    except Exception:
        pass
    s = cp["settings"] if "settings" in cp else {}
    def safe_getint(key, default):
        try:
            return int(s.get(key, default))
        except Exception:
            return default
    return {
        "concurrency": safe_getint("concurrency", 15),
        "wallpaper": s.get("wallpaper", "") or "",
        "live_cos_url": s.get("live_cos_url", "") or "",
    }

def fix_path_value(val, base):
    if not isinstance(val, str):
        return val
    if not val.startswith(("./", "../")):
        return val
    parts = val.split(";")
    parts[0] = urljoin(base, parts[0])
    return ";".join(parts)

def fix_dict_paths(d, base):
    if isinstance(d, dict):
        for k, v in d.items():
            if isinstance(v, str):
                d[k] = fix_path_value(v, base)
            elif isinstance(v, dict):
                fix_dict_paths(v, base)
            elif isinstance(v, list):
                for i, item in enumerate(v):
                    if isinstance(item, str):
                        v[i] = fix_path_value(item, base)
                    elif isinstance(item, dict):
                        fix_dict_paths(item, base)
    return d

def fix_site_paths(site, base):
    if not isinstance(site, dict):
        return site
    for f in ("api", "ext", "jar"):
        if f in site:
            v = site[f]
            if isinstance(v, str):
                site[f] = fix_path_value(v, base)
            elif isinstance(v, dict):
                fix_dict_paths(v, base)
    return site

def safe_spider(spider):
    if not isinstance(spider, str):
        return ""
    return spider

def safe_url(u):
    if not isinstance(u, str):
        return ""
    u = u.strip()
    if not u.startswith(("http://", "https://")):
        return ""
    return u

def fetch_recursive(item, depth=0, src=None):
    name, url = item["name"], item["url"]
    # 顶层来源名：若未指定则用本源的名称（兼容聚合子源继承）
    if src is None:
        src = name
    try:
        ok, code, text = http_get(url, timeout=15)
        if not ok: return None, f"HTTP {code}"
        data = parse_json_lenient(text)
        if not isinstance(data, dict):
            return None, "返回不是JSON对象"
        if data.get("msg"):
            return None, f"msg: {data['msg']}"
    except Exception as e:
        return None, f"解析错误: {str(e)[:60]}"

    try:
        if "urls" in data and isinstance(data["urls"], list):
            if depth >= 2: return None, "嵌套太深"
            all_sites, all_parses = [], []
            spider_set = set()
            site_base_map = {}
            for sub in data["urls"]:
                if not isinstance(sub, dict): continue
                su = safe_url(sub.get("url", ""))
                if not su: continue
                sn = sub.get("name", "")
                if not isinstance(sn, str) or not sn:
                    sn = su[:30]
                result, err = fetch_recursive({"name": sn, "url": su}, depth + 1, src)
                if err:
                    print(f"    X {sn}: {err}")
                    continue
                sub_sites, sub_parses, sub_spiders, sub_base = result
                all_sites.extend(sub_sites)
                all_parses.extend(sub_parses)
                for sp in sub_spiders:
                    sp = safe_spider(sp)
                    if sp: spider_set.add(sp)
                # 子源返回的 sub_base 已映射每个站点到其真实归属源地址，
                # 这里合并即可，切勿用子源聚合地址覆盖（会导致二次 fix base 错位）
                if sub_base:
                    site_base_map.update(sub_base)
                print(f"    OK {sn} ({len(sub_sites)}站)")
            return (all_sites, all_parses, list(spider_set), site_base_map), None

        sites = data.get("sites", [])
        if not isinstance(sites, list):
            sites = []
        spider = safe_spider(data.get("spider", ""))
        for s in sites:
            fix_site_paths(s, url)
        if spider:
            spider = fix_path_value(spider, url)
        if spider:
            for s in sites:
                if isinstance(s, dict) and not s.get("jar"):
                    s["jar"] = spider
        site_base_map = {id(s): url for s in sites if isinstance(s, dict)}
        # 标注每个站点所属顶层来源名（供健康报告 / merge_health 按源统计）
        for s in sites:
            if isinstance(s, dict):
                s["_source"] = src
        parses = data.get("parses", [])
        if not isinstance(parses, list):
            parses = []
        spiders = [spider] if spider else []
        return (sites, parses, spiders, site_base_map), None
    except Exception as e:
        return None, f"处理错误: {str(e)[:60]}"

def build_health_output(all_sites):
    """生成 merge_health.json：按源分组展示站点成功/失败情况。
    结构: {"updated_at":..., "sources":[{"name":.., "ok":bool, "site_ok":n, "site_fail":n,
          "sites":[{"key":..,"name":..,"api":..,"ok":bool,"error":..}]}]}"""
    srcs = {}
    for s in all_sites:  # 遍历的每个站点是去重后保留的最终版本
        if not isinstance(s, dict):
            continue
        name = s.get("_source") or "未标注来源"
        rec = srcs.setdefault(name, [])

        urls = []
        for f in ("api", "ext", "jar"):
            v = s.get(f, "")
            if isinstance(v, str):
                head = v.split(";")[0].strip()
                if head:
                    urls.append(head)
        rec.append({
            # 不直接输出 key（可能是敏感/超长），用序号做站内标识
            "_i": len(rec),
            "name": s.get("name", ""),
            "api": urls,
            "ok": s.get("_pok", True),
            "error": s.get("_perr", ""),
        })
    sources = []
    for name, sites in srcs.items():
        ok_n = sum(1 for x in sites if x["ok"])
        sources.append({
            "name": name,
            "total": len(sites),
            "ok": ok_n,
            "fail": len(sites) - ok_n,
            "sites": sites,
        })
    payload = {"updated_at": now_str(), "sources": sources}
    try:
        with open(HEALTH_JSON_FILE, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"  写 {HEALTH_JSON_FILE} 失败: {e}")

def main():
    t0 = time.time()
    print(f"=== TVBox 合并 {now_str()} ===")
    cfg = load_config()
    remote, lives = load_sources()
    print(f"点播源{len(remote)}个, 直播{len(lives)}条")

    print("拉取点播源...")
    all_sites, all_parses = [], []
    all_spiders = set()
    site_base_map = {}
    errors = []
    with ThreadPoolExecutor(max_workers=cfg["concurrency"]) as ex:
        futs = {ex.submit(fetch_recursive, s): s for s in remote}
        for f in as_completed(futs):
            try:
                result, err = f.result()
            except Exception as e:
                errors.append(f"任务异常: {e}")
                print(f"  X 任务异常: {e}")
                continue
            if err:
                errors.append(f"{err}")
                print(f"  X {err}")
                continue
            sites, parses, spiders, bases = result
            all_sites.extend(sites)
            all_parses.extend(parses)
            for sp in spiders:
                sp = safe_spider(sp)
                if sp: all_spiders.add(sp)
            site_base_map.update(bases)
            print(f"  OK: {len(sites)}站")

    fixed = 0
    for s in all_sites:
        if not isinstance(s, dict): continue
        base = site_base_map.get(id(s), "")
        if not base: continue
        for f in ("api", "ext", "jar"):
            v = s.get(f, "")
            if isinstance(v, str) and v.startswith(("./", "../")):
                s[f] = fix_path_value(v, base)
                fixed += 1
            elif isinstance(v, dict):
                fix_dict_paths(v, base)
                fixed += 1
    if fixed:
        print(f"  兜底补全 {fixed} 处相对路径")

    for s in all_sites:
        if isinstance(s, dict):
            s["name"] = sanitize_site_name(s.get("name", ""))

    # ---- 先探活，再去重 ----
    # 探活必须发生在去重之前：这样同 key 有多个候选时，
    # 能用“接口是否可用”优先选优，避免“留下死链、丢掉正常”站点。
    print("站点探活...")
    probe_ok = {}; probe_info = {}
    with ThreadPoolExecutor(max_workers=cfg["concurrency"]) as ex:
        futs = {ex.submit(probe_site, s): s for s in all_sites if isinstance(s, dict)}
        for f in as_completed(futs):
            s = futs[f]
            try:
                ok, err = f.result()
            except Exception as e:
                ok, err = False, str(e)[:80]
            probe_ok[id(s)] = ok
            probe_info[id(s)] = err

    # ---- 站点去重 ----
    # TVBox 客户端以 key（无 key 时用 name）作为站点唯一标识加载。
    # 同 key 保留“探活通过且更完整”的实现：
    #   优先级① 探活 ok（接口可用）
    #   优先级② 字段更完整（spider/api/ext/jar 非空数量更多）
    core_fields = ("spider", "api", "ext", "jar")
    merged_sites = {}

    def set_probe(d, s):
        d["_pok"] = probe_ok.get(id(s), True)
        d["_perr"] = probe_info.get(id(s), "")

    def completeness(x):
        return sum(1 for f in core_fields if x.get(f) not in (None, "", {}))

    def better(a, b):
        """a 是否应替换 b。ok 优先于字段完整性。"""
        pa = probe_ok.get(id(a), True)
        pb = probe_ok.get(id(b), True)
        if pa != pb:
            return pa and not pb
        return completeness(a) > completeness(b)

    for s in all_sites:
        if not isinstance(s, dict):
            continue
        key = s.get("key") or s.get("name")
        if not key:
            continue
        if key not in merged_sites:
            merged_sites[key] = dict(s)
            set_probe(merged_sites[key], s)
            continue
        # 同 key：接口可用性优先，其次字段完整性，用更优版本做底
        if better(s, merged_sites[key]):
            merged_sites[key] = dict(s)
            set_probe(merged_sites[key], s)
        for f in core_fields:
            if merged_sites[key].get(f) in (None, "", {}) and (s.get(f) not in (None, "", {})):
                merged_sites[key][f] = s[f]
                # 回填字段可能导致可用性变化，保持以底版本探活结果为准
    all_sites = list(merged_sites.values())
    print(f"  站点去重后 {len(all_sites)} 站")

    # 用最终站点的探活结果生成 merge_health.json（从站点自带 _pok/_perr 读取）
    build_health_output(all_sites)
    # 移除内部字段与探活标记，避免写入 merged.json
    for s in all_sites:
        if isinstance(s, dict):
            s.pop("_source", None)
            s.pop("_pok", None)
            s.pop("_perr", None)

    pmerged = {}; porder = []
    for src in all_parses:
        if not isinstance(src, list): continue
        for it in src:
            if not isinstance(it, dict): continue
            sid = hashlib.md5(f"{it.get('name','')}|{it.get('url','')}|{it.get('api','')}".encode()).hexdigest()
            if sid not in pmerged:
                pmerged[sid] = it; porder.append(sid)
    parses = [pmerged[k] for k in porder]

    global_spider = ""
    if len(all_spiders) == 1:
        global_spider = list(all_spiders)[0]
        print(f"  所有源 spider 一致: {global_spider}")
    elif len(all_spiders) > 1:
        print(f"  多个不同 spider（{len(all_spiders)}个），不写顶层")

    print("处理直播...")
    live_count, live_lines = process_live_sources([lv["url"] for lv in lives], cfg["concurrency"])

    live_cos = cfg.get("live_cos_url", "")
    lives_field = [{"name": "央视+卫视", "type": 0, "url": live_cos}] if live_cos else lives

    try:
        merged = {
            "spider": global_spider,
            "sites": all_sites,
            "lives": lives_field,
            "parses": parses,
            "wallpaper": cfg["wallpaper"],
            "update_time": now_str(),
            "version": "4.9"
        }
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(merged, f, ensure_ascii=False, indent=2)

        with open(REPORT_FILE, "w", encoding="utf-8") as f:
            f.write(f"# TVBox 订阅源报告\n\n**时间**: {now_str()}\n\n")
            f.write(f"**站点**: {len(all_sites)}\n\n")
            f.write(f"**直播**: {live_count}频道/{live_lines}线路\n\n")
            f.write(f"**spider**: `{global_spider}`\n\n")
            f.write(f"**耗时**: {int(time.time()-t0)}秒\n\n")
            if errors:
                f.write("## 拉取失败的源\n\n")
                for e in errors: f.write(f"- {e}\n")
    except Exception as e:
        print(f"  写文件失败: {e}")
        sys.exit(1)

    print(f"=== 完成: 站点{len(all_sites)} 直播{live_count}频道 耗时{int(time.time()-t0)}秒 ===")

if __name__ == "__main__":
    main()
