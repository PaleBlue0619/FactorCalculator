# def complete_data_paths_iterative(factor_cfg):
#     """
#     使用迭代方式为每个因子补全dataPath信息
#     收集所有底层依赖的dataPath并合并到当前因子的dataPath中
#     """
#     factor_map = {name: cfg.copy() for name, cfg in factor_cfg.items()}
#
#     # 构建依赖关系
#     dependency_map = {}
#     for factor_name, cfg in factor_map.items():
#         deps = cfg['dependency']['factor']
#         if deps is None:
#             deps = []
#         elif isinstance(deps, str):
#             deps = [deps]
#         dependency_map[factor_name] = deps
#
#     # 为每个因子找到所有底层依赖的dataPath
#     for factor_name in factor_map:
#         cfg = factor_map[factor_name]
#
#         # 使用集合来跟踪访问过的因子，避免循环依赖
#         visited = set()
#         current_deps = dependency_map[factor_name][:]
#         all_data_paths = []
#
#         # 如果当前因子自己有dataPath，先加入
#         if cfg['dataPath']:
#             all_data_paths.extend(cfg['dataPath'])
#
#         while current_deps:
#             dep_factor = current_deps.pop(0)
#
#             # 检查循环依赖
#             if dep_factor in visited:
#                 raise ValueError(f"检测到循环依赖: {factor_name} -> {dep_factor}")
#             visited.add(dep_factor)
#
#             dep_cfg = factor_map[dep_factor]
#
#             # 如果依赖的因子有dataPath，直接收集
#             if dep_cfg['dataPath']:
#                 all_data_paths.extend(dep_cfg['dataPath'])
#
#             # 无论依赖的因子是否有dataPath，都要继续追溯它的依赖
#             # 因为可能依赖的因子本身没有dataPath，但它依赖的其他因子有
#             current_deps.extend(dependency_map[dep_factor])
#
#         # 去重并保持顺序
#         unique_paths = []
#         seen_paths = set()
#         for path in all_data_paths:
#             if path not in seen_paths:
#                 seen_paths.add(path)
#                 unique_paths.append(path)
#
#         cfg['dataPath'] = unique_paths
#
#     return factor_map
#
#
# # 测试函数
# def test_complete_data_paths():
#     factor_cfg = {
#         "shio": {
#             "class": "shio",
#             "calFunc": "get_shio",
#             "dependency": {"factor": None, "midFunc": ["shioFunc"]},
#             "dataPath": ["Min1KBar", "DayKBar"],
#             "indicator": [["open", "close", "volume"], ["open", "close"]],
#             "params": {"freq": "day", "callBackPeriod": 0}
#         },
#         "shio_avg20": {
#             "class": "shio",
#             "calFunc": "get_shio_avg20",
#             "dependency": {"factor": ["shio"], "midFunc": None},
#             "dataPath": ["Min1KBar2"],  # 这个应该被保留，并与依赖的dataPath合并
#             "indicator": [],
#             "params": {"freq": "day", "callBackPeriod": 20}
#         },
#         "shio_std20": {
#             "class": "shio",
#             "calFunc": "get_shio_std20",
#             "dependency": {"factor": ["shio"], "midFunc": None},
#             "dataPath": [],  # 这个应该被补充为依赖的dataPath
#             "indicator": [],
#             "params": {"freq": "day", "callBackPeriod": 20}
#         }
#     }
#
#     print("原始配置:")
#     for name, cfg in factor_cfg.items():
#         print(f"  {name}: dataPath={cfg['dataPath']}")
#
#     print("\n补全后的配置 (迭代版本):")
#     result_iterative = complete_data_paths_iterative(factor_cfg)
#     for name, cfg in result_iterative.items():
#         print(f"  {name}: dataPath={cfg['dataPath']}")
#
#
# if __name__ == "__main__":
#     test_complete_data_paths()

import networkx as nx
from typing import List, Dict


def simple_dependency_sort(factor_cfg: Dict, factor_list: List[str]) -> List[str]:
    """
    简单的依赖关系排序
    """
    G = nx.DiGraph()
    G.add_nodes_from(factor_list)

    # 构建依赖图
    for factor in factor_list:
        if factor in factor_cfg:
            deps = factor_cfg[factor]['dependency']['factor']
            if deps is None:
                continue
            if isinstance(deps, str):
                deps = [deps]
            for dep in deps:
                if dep in factor_list:
                    G.add_edge(dep, factor)

    try:
        return list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        # 循环依赖时返回原始列表
        print("警告: 存在循环依赖，返回原始顺序")
        return factor_list


# 使用示例
if __name__ == "__main__":
    # 示例配置
    factor_cfg = {
        'factor_a': {'dependency': {'factor': ['factor_b', 'factor_c']}, 'dataPath': []},
        'factor_b': {'dependency': {'factor': ['factor_d']}, 'dataPath': []},
        'factor_c': {'dependency': {'factor': ['factor_e']}, 'dataPath': []},
        'factor_d': {'dependency': {'factor': None}, 'dataPath': []},
        'factor_e': {'dependency': {'factor': ['factor_d']}, 'dataPath': []},
        'factor_f': {'dependency': {'factor': None}, 'dataPath': []},
        'factor_g': {'dependency': {'factor': ['factor_a']}, 'dataPath': []}
    }

    factor_list = ['factor_a', 'factor_b', 'factor_c', 'factor_d', 'factor_e', 'factor_f', 'factor_g']

    print("基础拓扑排序:")
    result1 = simple_dependency_sort(factor_cfg, factor_list)
    print(result1)