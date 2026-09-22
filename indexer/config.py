"""索引服务配置项。"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class IndexerConfig:
    """运行期配置。

    max_reorg_depth: 允许的最大重组深度，超过即拒绝。
    genesis_hash: 本地固定数据的创世块哈希，作为规范链的根。
    db_path: SQLite 持久化文件路径，":memory:" 表示纯内存。
    """

    max_reorg_depth: int = 6
    genesis_hash: str = ""
    db_path: str = ":memory:"
