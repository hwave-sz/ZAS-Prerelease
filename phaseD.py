#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Phase D · v7 真实 GPU 验收与 utility_v2 消融（自包含，无第三方依赖）

设计原则（写死在代码里，避免事后解释）：
1. 四臂同模型/同硬件/同副本/同负载/同引擎配置，只有被测变量不同：
     A  direct    —— 客户端轮询直连 8001/8002（官方形态基线，不经网关）
     B  two_choices —— v7 网关，reuse.independent=true（纯代理 + 旧策略）
     C  utility_v2  —— 同 B，仅换策略（隔离新算法净收益）
2. 每臂前重启网关（清 L0/L1/M1 状态），避免上臂残留污染。
3. 负载用固定 seed 生成，四臂完全相同的请求序列（含重复前缀簇，dup≈0.5）。
4. 直连臂与网关臂使用同一 bench 代码，差别只在 base_url。
5. 采集 /_route_state（网关臂），用于事后判读"负载是否真的改变了选择"。

用法：
  python3 phaseD.py <arm> <concurrency> <duration_s> [tag]
  python3 phaseD.py report            # 汇总所有臂
"""
import json
import os
import random
import re
import shutil
import signal
import socket
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

GW_BIN = "/tmp/gw7"
BASE_CFG_CANDIDATES = [
    "/mnt/workspace/gw3/gw_c.yaml",
    "/mnt/workspace/gw3/config.yaml",
    "/mnt/workspace/gw/gw_c.yaml",
    "/mnt/workspace/gw_c.yaml",
]
OUT = "/tmp/phaseD"
UPSTREAMS = ["http://127.0.0.1:8001", "http://127.0.0.1:8002"]
GW_PORT = 18195
GW = "http://127.0.0.1:%d" % GW_PORT

# ---------------- 负载生成（固定 seed，四臂一致） ----------------
PREFIX_POOL = [
    "你是一个严谨的技术助理，请基于以下背景回答问题。背景：某推理网关需要在高并发下同时满足首字延迟与输出速率两项指标，团队已经实现了前缀缓存、流式同流、容量准入与多维感知路由四层机制。",
    "以下是系统设计文档的片段。文档说明：网关在准入层使用信用账本，调度层使用两两比较，缓存层使用精确键与前缀索引，传输层使用合并广播。请据此回答。",
    "请阅读下面这段代码并说明它的作用。代码：func admit(req Request) (Decision, error) { if err := check(req); err != nil { return Decision{}, err }; return sched(req) }",
    "背景材料：在过去三个月中，团队围绕 TTFT P99 与 TPOT 两项指标做了多轮实验，发现二者存在结构性张力：降低排队会牺牲批处理规模，提升批处理规模会拉高尾部延迟。",
]
QUESTIONS = [
    "请用三句话总结上述背景的核心矛盾。",
    "请列出两个可能的优化方向，并说明各自的代价。",
    "如果把并发从 24 提升到 32，你认为哪个指标最先恶化？为什么？",
    "请指出上述代码缺少哪些错误处理。",
    "请用一段话解释什么是排队放大效应。",
]
LONG_TAIL = "请展开说明，并给出一个具体的数值例子来支撑你的结论。"


def build_workload(n, seed=20261127):
    """生成 n 条请求规格：40% 长输出/60% 短输出；50% 复用前缀簇（模拟 official dup）。"""
    rnd = random.Random(seed)
    specs = []
    for i in range(n):
        dup = (i % 2 == 0)  # 偶数位走复用簇
        if dup:
            p = PREFIX_POOL[(i // 2) % len(PREFIX_POOL)]
        else:
            p = PREFIX_POOL[rnd.randrange(len(PREFIX_POOL))]
        q = QUESTIONS[rnd.randrange(len(QUESTIONS))]
        long_out = rnd.random() < 0.4
        max_tokens = 256 if long_out else 64
        prompt = p + "\n\n问题：" + q + ("\n" + LONG_TAIL if long_out else "")
        specs.append({"key": i, "dup": dup, "prompt": prompt, "max_tokens": max_tokens})
    return specs


# ---------------- HTTP（流式） ----------------
def one_request(base, spec, model, deadline_s, sink):
    """发一条流式请求，记录 TTFT / 逐 token 间隔 / e2e。"""
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": spec["prompt"]}],
        "max_tokens": spec["max_tokens"],
        "temperature": 0,
        "stream": True,
    }).encode()
    req = urllib.request.Request(base + "/v1/chat/completions", data=body,
                                 headers={"Content-Type": "application/json"})
    t0 = time.time()
    rec = {"key": spec["key"], "dup": spec["dup"], "max_tokens": spec["max_tokens"],
           "offered_s": round(t0 - T_ARM0, 3), "ok": False, "first_ms": None,
           "end_ms": None, "tokens": 0, "tpot_ms": None, "err": None, "inst": None,
           "ttft_stamps": []}
    try:
        with urllib.request.urlopen(req, timeout=deadline_s) as r:
            rec["inst"] = r.headers.get("X-Upstream-Inst")
            rec["cache"] = r.headers.get("X-Cache")
            buf = b""
            stamps = []
            ntok = 0
            t_first = None
            while True:
                chunk = r.read(4096)
                if not chunk:
                    break
                if t_first is None:
                    t_first = time.time() - t0
                    rec["first_ms"] = round(t_first * 1000, 1)
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    line = line.strip()
                    if not line.startswith(b"data:"):
                        continue
                    payload = line[5:].strip()
                    if payload == b"[DONE]":
                        continue
                    try:
                        obj = json.loads(payload)
                    except Exception:
                        continue
                    ch = obj.get("choices") or []
                    if ch and (ch[0].get("delta") or {}).get("content"):
                        ntok += 1
                        stamps.append(time.time() - t0)
            t_end = time.time() - t0
            rec["tokens"] = ntok
            rec["end_ms"] = round(t_end * 1000, 1)
            if ntok >= 2 and stamps:
                # TPOT = 首 token 之后每个 token 的平均间隔（ms）
                span = stamps[-1] - stamps[0]
                rec["tpot_ms"] = round(span / (ntok - 1) * 1000, 2) if ntok > 1 else None
            rec["ok"] = ntok > 0
            if not rec["ok"]:
                rec["err"] = "zero_content"
    except urllib.error.HTTPError as e:
        rec["err"] = "http_%d" % e.code
        try:
            rec["body"] = e.read()[:200].decode("utf-8", "replace")
        except Exception:
            pass
    except Exception as e:
        rec["err"] = type(e).__name__ + ":" + str(e)[:80]
    rec["elapsed_s"] = round(time.time() - t0, 3)
    sink.append(rec)


T_ARM0 = 0.0


def run_bench(base, model, concurrency, duration_s, out_path):
    global T_ARM0
    specs = build_workload(100000)
    recs = []
    lock = threading.Lock()
    stop = threading.Event()
    T_ARM0 = time.time()
    deadline = T_ARM0 + duration_s

    def worker(wid):
        idx = wid
        while not stop.is_set() and time.time() < deadline:
            spec = specs[idx % len(specs)]
            idx += concurrency
            one_request(base, spec, model, max(30, duration_s), recs)
            if time.time() >= deadline:
                break

    ths = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(concurrency)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(timeout=duration_s + 90)
    stop.set()

    with open(out_path, "w") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return recs


# ---------------- 统计 ----------------
def pct(xs, q):
    if not xs:
        return None
    xs = sorted(xs)
    k = min(len(xs) - 1, max(0, int(round(q * (len(xs) - 1)))))
    return xs[k]


def summarize(recs, duration_s):
    ok = [r for r in recs if r["ok"]]
    ttft = [r["first_ms"] for r in ok if r["first_ms"]]
    tpot = [r["tpot_ms"] for r in ok if r["tpot_ms"]]
    e2e = [r["end_ms"] for r in ok if r["end_ms"]]
    toks = sum(r["tokens"] for r in ok)
    return {
        "n": len(recs), "ok": len(ok), "success": round(len(ok) / max(1, len(recs)) * 100, 2),
        "ttft_p50": pct(ttft, .5), "ttft_p95": pct(ttft, .95), "ttft_p99": pct(ttft, .99),
        "tpot_p50": pct(tpot, .5), "tpot_p95": pct(tpot, .95), "tpot_p99": pct(tpot, .99),
        "e2e_p50": pct(e2e, .5), "e2e_p99": pct(e2e, .99),
        "out_tok_per_s": round(toks / duration_s, 1),
        "errs": {e: sum(1 for r in recs if r["err"] == e) for e in
                 {r["err"] for r in recs if r["err"]}},
    }


# ---------------- 网关控制 ----------------
def find_base_cfg():
    for p in BASE_CFG_CANDIDATES:
        if os.path.isfile(p):
            return p
    hits = []
    for root in ("/mnt/workspace",):
        for dirpath, _, files in os.walk(root):
            if dirpath.count(os.sep) - root.count(os.sep) > 2:
                continue
            for f in files:
                if f.endswith(".yaml") and ("gw_c" in f or "config" in f):
                    hits.append(os.path.join(dirpath, f))
    return hits[0] if hits else None


def patch_yaml(src, dst, strategy, independent, port):
    """在文本层改三处，避免依赖 PyYAML 的具体版本行为。"""
    txt = open(src, encoding="utf-8").read()
    # listen_addr
    if re.search(r"^\s*listen_addr:", txt, re.M):
        txt = re.sub(r"^(\s*listen_addr:\s*).*$", r'\1"127.0.0.1:%d"' % port, txt, count=1, flags=re.M)
    else:
        txt = "server:\n  listen_addr: \"127.0.0.1:%d\"\n" % port + txt
    # router.strategy
    if re.search(r"^router:\s*$", txt, re.M):
        if re.search(r"^\s*strategy:", txt, re.M):
            txt = re.sub(r"^(\s*strategy:\s*).*$", r"\1" + strategy, txt, count=1, flags=re.M)
        else:
            txt = re.sub(r"^(router:\s*)$", r"\1\n  strategy: " + strategy, txt, count=1, flags=re.M)
    else:
        txt = txt + "\nrouter:\n  strategy: %s\n" % strategy
    # reuse.independent
    if re.search(r"^reuse:\s*$", txt, re.M):
        if re.search(r"^\s*independent:", txt, re.M):
            txt = re.sub(r"^(\s*independent:\s*).*$", r"\1%s" % ("true" if independent else "false"),
                         txt, count=1, flags=re.M)
        else:
            txt = re.sub(r"^(reuse:\s*)$", r"\1\n  independent: %s" % ("true" if independent else "false"),
                         txt, count=1, flags=re.M)
    else:
        txt = txt + "\nreuse:\n  independent: %s\n" % ("true" if independent else "false")
    open(dst, "w", encoding="utf-8").write(txt)
    return dst


def wait_port(port, timeout=25):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            s = socket.create_connection(("127.0.0.1", port), timeout=1)
            s.close()
            return True
        except Exception:
            time.sleep(0.3)
    return False


def start_gw(cfg):
    log = open("/tmp/phaseD/gw.log", "a")
    p = subprocess.Popen([GW_BIN, "-config", cfg], stdout=log, stderr=log,
                         preexec_fn=os.setsid)
    return p


def stop_gw(p):
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
        p.wait(timeout=10)
    except Exception:
        try:
            os.killpg(os.getpgid(p.pid), signal.SIGKILL)
        except Exception:
            pass


def get_route_state(tag):
    try:
        with urllib.request.urlopen(GW + "/_route_state", timeout=10) as r:
            return r.read().decode("utf-8", "replace")
    except Exception as e:
        return "route_state_error: %s" % e


def detect_model(cfg_path):
    txt = open(cfg_path, encoding="utf-8").read()
    m = re.search(r"^\s*model:\s*\"?([\w\-.]+)\"?\s*$", txt, re.M)
    return m.group(1) if m else "qwen2.5-32b"


def arm_direct(conc, dur, tag):
    model = detect_model(find_base_cfg())
    recs = []
    # 直连：客户端轮询两个副本（官方形态：客户端健康 RR）
    specs = build_workload(100000)
    global T_ARM0
    stop = threading.Event()
    T_ARM0 = time.time()
    deadline = T_ARM0 + dur

    def worker(wid):
        idx = wid
        while not stop.is_set() and time.time() < deadline:
            spec = specs[idx % len(specs)]
            base = UPSTREAMS[(idx // 1) % len(UPSTREAMS)]
            idx += conc
            one_request(base, spec, model, max(30, dur), recs)
            if time.time() >= deadline:
                break

    ths = [threading.Thread(target=worker, args=(i,), daemon=True) for i in range(conc)]
    for t in ths:
        t.start()
    for t in ths:
        t.join(timeout=dur + 90)
    stop.set()
    with open(os.path.join(OUT, "%s.jsonl" % tag), "w") as f:
        for r in recs:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    return recs


def main():
    os.makedirs(OUT, exist_ok=True)
    if len(sys.argv) < 2:
        print(__doc__)
        return 1
    arm = sys.argv[1]
    if arm == "report":
        return report()
    conc = int(sys.argv[2])
    dur = int(sys.argv[3])
    tag = sys.argv[4] if len(sys.argv) > 4 else "%s_c%d" % (arm, conc)
    base_cfg = find_base_cfg()
    print("[cfg] base =", base_cfg)
    model = detect_model(base_cfg)
    print("[cfg] model =", model)

    if arm == "direct":
        recs = arm_direct(conc, dur, tag)
        summ = summarize(recs, dur)
        json.dump(summ, open(os.path.join(OUT, "%s.summary.json" % tag), "w"), indent=2)
        print("[%s] %s" % (tag, json.dumps(summ, ensure_ascii=False)))
        return 0

    strategy = {"B": "two_choices", "C": "utility_v2"}.get(arm, arm)
    independent = True  # B/C 都是独立生成模式（隔离路由变量）
    cfg = patch_yaml(base_cfg, "/tmp/phaseD/cfg_%s.yaml" % tag, strategy, independent, GW_PORT)
    print("[cfg] patched ->", cfg)
    p = start_gw(cfg)
    try:
        if not wait_port(GW_PORT, 30):
            print("[FATAL] 网关未起来，见 /tmp/phaseD/gw.log")
            return 2
        # 健康检查 + 预热一次（不纳入统计）
        try:
            urllib.request.urlopen(GW + "/healthz", timeout=5).read()
        except Exception as e:
            print("[warn] healthz:", e)
        recs = run_bench(GW, model, conc, dur, os.path.join(OUT, "%s.jsonl" % tag))
        summ = summarize(recs, dur)
        rs = get_route_state(tag)
        open(os.path.join(OUT, "%s.route_state.txt" % tag), "w").write(rs)
        dim = [l for l in rs.splitlines() if l.startswith("route_utility_v2")]
        summ["route_utility_v2"] = dim[0] if dim else None
        json.dump(summ, open(os.path.join(OUT, "%s.summary.json" % tag), "w"), indent=2)
        print("[%s] %s" % (tag, json.dumps(summ, ensure_ascii=False)))
    finally:
        stop_gw(p)
    return 0


def report():
    rows = []
    for f in sorted(os.listdir(OUT)):
        if f.endswith(".summary.json"):
            rows.append((f[:-13], json.load(open(os.path.join(OUT, f)))))
    if not rows:
        print("no arms yet")
        return 1
    hdr = "%-14s %5s %6s %8s %8s %8s %8s %8s %8s" % (
        "arm", "n", "succ%", "ttft50", "ttft99", "tpot50", "tpot99", "e2e99", "tok/s")
    print(hdr)
    print("-" * len(hdr))
    for tag, s in rows:
        print("%-14s %5d %6.2f %8s %8s %8s %8s %8s %8s" % (
            tag, s["n"], s["success"], s["ttft_p50"], s["ttft_p99"],
            s["tpot_p50"], s["tpot_p99"], s["e2e_p99"], s["out_tok_per_s"]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
