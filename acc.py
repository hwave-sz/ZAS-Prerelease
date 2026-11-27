#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""遥测链路确定性验收（不跑性能，只验链路）。

四个验收项（对应负责人 09:14 指令）：
  A1 两个实例有**独立**遥测来源     → 正反切换时两候选的 kv/running 必须不同
  A2 运行压力传到候选代价           → penalty 必须随压力变化（不是恒 1.04）
  A3 压力 A→B 时选择跟着变          → chosen 必须翻转
  A4 遥测缺失/过期 → 退回保守        → 停压后 penalty 相等且中性，不误判为"很空闲"

做法：单请求探针（不在负载下测性能），每阶段 5 发，间隔 3s，逐发打印审计行。
"""
import json
import os
import signal
import subprocess
import sys
import time
import urllib.request

D = "/tmp/ZAS-Prerelease-relay-gw7"
sys.path.insert(0, D)
import phaseD as P  # noqa: E402


def hog(port, conc=8, max_tok=384, dur=180):
    return subprocess.Popen(
        ["python3", os.path.join(D, "hog.py"), str(port), str(conc), str(max_tok), str(dur)],
        stdout=open("/tmp/hog_acc.log", "a"), stderr=subprocess.STDOUT,
        preexec_fn=os.setsid)


def kill(p):
    if p is None:
        return
    try:
        os.killpg(os.getpgid(p.pid), signal.SIGTERM)
    except Exception:
        pass
    try:
        p.wait(timeout=8)
    except Exception:
        pass


def probe(model):
    body = json.dumps({
        "model": model,
        "messages": [{"role": "user", "content": "验收探针：一句话说明负载感知路由的作用。"}],
        "max_tokens": 32, "temperature": 0, "stream": True,
        "stream_options": {"include_usage": True},
    }).encode()
    r = urllib.request.Request(P.GW + "/v1/chat/completions", data=body,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r, timeout=90) as resp:
        for _ in resp:
            pass


def lastdim():
    with urllib.request.urlopen(P.GW + "/_route_state", timeout=10) as r:
        ls = [l for l in r.read().decode("utf-8", "replace").splitlines()
              if l.startswith("last_dim")]
    return ls[-1] if ls else "(none)"


def step(name, model, n=5, gap=3):
    print("\n### %s" % name, flush=True)
    for i in range(n):
        try:
            probe(model)
        except Exception as e:
            print("  probe err: %s" % e, flush=True)
        print("  %s" % lastdim(), flush=True)
        time.sleep(gap)


def main():
    base = P.find_base_cfg()
    model = P.detect_model(base)
    os.environ["KVSH"] = "1"
    cfg = P.patch_yaml(base, "/tmp/phaseD/cfg_acc.yaml", "utility_v2", True, P.GW_PORT)
    print("[cfg] %s -> %s" % (base, cfg), flush=True)
    gw = P.start_gw(cfg)
    h = None
    try:
        if not P.wait_port(P.GW_PORT, 30):
            print("[FATAL] 网关未起，见 /tmp/phaseD/gw.log")
            return 2
        step("阶段0 无压力（基线：两候选应同值、penalty 中性）", model, n=3, gap=2)

        h = hog(8001)
        time.sleep(12)
        step("阶段1 压 8001（期望 chosen=02，两候选 kv/penalty 出现差异）", model, n=5)
        kill(h)

        time.sleep(5)
        h = hog(8002)
        time.sleep(12)
        step("阶段2 改压 8002（期望 chosen=01，差异方向反转）", model, n=5)
        kill(h)

        time.sleep(10)  # 等遥测过期（MaxAge 默认 6s）
        step("阶段3 停压 10s 后（期望退回保守：penalty 中性、不把过期当空闲）", model, n=3, gap=2)
    finally:
        kill(h)
        P.stop_gw(gw)
        print("\n=== ACCEPT DONE ===", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
