# tvbox-sub

TVBox 点播源 + 直播源自动聚合订阅源。

## 输出
- `merged.json` — 合并后的点播 + 直播订阅
- `live.txt` — 直播频道列表
- `merge_health.json` — 站点健康探活数据（由 CI 生成）
- `index.html` — 站点健康可视化看板

## 健康看板

📡 可交互查看各订阅源的站点健康情况：https://wangguo0.github.io/tvbox-sub/

## 更新
由 GitHub Actions 每 3 天自动合并并发布到 GitHub Pages。