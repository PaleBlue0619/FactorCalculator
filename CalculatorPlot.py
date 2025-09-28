import json
import json5
import dolphindb as ddb
from typing import Dict, List, Set
from pyecharts import options as opts
from pyecharts.charts import Graph, Page
from Calculator import *
import networkx as nx

# 这里是主函数中设置的变量名称
config = {
    "dayFactorDB": "dfs://Dayfactor",
    "dayFactorTB": "pt",
    "minFactorDB": "dfs://Minfactor",
    "minFactorTB": "pt",
    "sourceDict": "sourceDict",
    "sourceObj": "df",
    "middleObj": "middle",
    "dataObj": "data",
    "factorDict": "factorDict",
    "symbolCol": "symbol",
    "dateCol": "TradeDate",
    "timeCol": "TradeTime",
    "minuteCol": "minute",
}


class FactorVisualizer:
    def __init__(self, factor_calculator: FactorCalculator):
        self.fc = factor_calculator
        self.nodes = []
        self.links = []
        # 修正：在categories中直接定义颜色和样式
        self.categories = [
            {"name": "日频因子", "itemStyle": {"color": "#5470c6"}},  # 蓝色
            {"name": "分钟频因子", "itemStyle": {"color": "#91cc75"}},  # 绿色
            {"name": "classFunc", "itemStyle": {"color": "#fac858"}},  # 黄色
            {"name": "midFunc", "itemStyle": {"color": "#73c0de"}},  # 浅蓝色
            {"name": "数据源", "itemStyle": {"color": "#9a60b4"}}  # 紫色
        ]

    def _get_node_category(self, node_type: str, freq: str = None) -> int:
        """获取节点类别索引"""
        if node_type == "classFunc":
            return 2
        elif node_type == "midFunc":
            return 3
        elif node_type == "dataSource":
            return 4
        elif freq == "day":
            return 0
        elif freq == "minute":
            return 1
        else:
            return 0

    def _get_node_symbol(self, node_type: str) -> str:
        """获取节点形状"""
        symbol_map = {
            "factor": "circle",
            "classFunc": "rect",
            "midFunc": "diamond",
            "dataSource": "roundRect"
        }
        return symbol_map.get(node_type, "circle")

    def _get_node_size(self, node_type: str) -> int:
        """获取节点大小"""
        size_map = {
            "factor": 40,
            "classFunc": 35,
            "midFunc": 30,
            "dataSource": 25
        }
        return size_map.get(node_type, 40)

    def _add_factor_nodes(self):
        """添加因子节点"""
        for factor_name, cfg in self.fc.factor_cfg.items():
            freq = cfg["params"]["freq"]
            class_name = cfg["class"]

            # 构建节点标签
            tooltip = f"""
            <b>{factor_name}</b><br/>
            类别: {class_name}<br/>
            频率: {freq}<br/>
            计算函数: {cfg['calFunc']}<br/>
            数据表: {len(cfg['dataPath'])}个<br/>
            中间函数: {cfg['dependency']['midFunc'] or '无'}
            """

            self.nodes.append({
                "name": factor_name,
                "symbolSize": self._get_node_size("factor"),
                "symbol": self._get_node_symbol("factor"),
                "category": self._get_node_category("factor", freq),
                "tooltip": opts.TooltipOpts(formatter=tooltip)
            })

    def _add_class_func_nodes(self):
        """添加classFunc节点"""
        for class_name, func_list in self.fc.class_cfg.items():
            if func_list:
                for func_name in func_list:
                    tooltip = f"""
                    <b>classFunc: {func_name}</b><br/>
                    类别: {class_name}<br/>
                    类型: 预处理函数
                    """

                    self.nodes.append({
                        "name": f"classFunc_{class_name}_{func_name}",
                        "symbolSize": self._get_node_size("classFunc"),
                        "symbol": self._get_node_symbol("classFunc"),
                        "category": self._get_node_category("classFunc"),
                        "tooltip": opts.TooltipOpts(formatter=tooltip)
                    })

    def _add_mid_func_nodes(self):
        """添加midFunc节点"""
        mid_funcs = set()
        for factor_cfg in self.fc.factor_cfg.values():
            if factor_cfg['dependency']['midFunc']:
                for mid_func in factor_cfg['dependency']['midFunc']:
                    mid_funcs.add(mid_func)

        for mid_func in mid_funcs:
            tooltip = f"""
            <b>midFunc: {mid_func}</b><br/>
            类型: 中间计算函数
            """

            self.nodes.append({
                "name": f"midFunc_{mid_func}",
                "symbolSize": self._get_node_size("midFunc"),
                "symbol": self._get_node_symbol("midFunc"),
                "category": self._get_node_category("midFunc"),
                "tooltip": opts.TooltipOpts(formatter=tooltip)
            })

    def _add_data_source_nodes(self):
        """添加数据源节点"""
        data_sources = set()
        for factor_cfg in self.fc.factor_cfg.values():
            for data_path in factor_cfg['dataPath']:
                data_sources.add(data_path)

        for data_source in data_sources:
            tooltip = f"""
            <b>数据源: {data_source}</b><br/>
            类型: 原始数据表
            """

            self.nodes.append({
                "name": f"data_{data_source}",
                "symbolSize": self._get_node_size("dataSource"),
                "symbol": self._get_node_symbol("dataSource"),
                "category": self._get_node_category("dataSource"),
                "tooltip": opts.TooltipOpts(formatter=tooltip)
            })

    def _add_factor_dependencies(self):
        """添加因子间依赖关系"""
        for factor_name, cfg in self.fc.factor_cfg.items():
            deps = cfg['dependency']['factor']
            if deps:
                if isinstance(deps, str):
                    deps = [deps]
                for dep in deps:
                    if dep in self.fc.factor_cfg:
                        self.links.append({
                            "source": dep,
                            "target": factor_name,
                            "lineStyle": {"color": "#5470c6", "width": 2}
                        })

    def _add_class_func_links(self):
        """添加classFunc连接"""
        class_to_factors = {}
        for factor_name, cfg in self.fc.factor_cfg.items():
            class_name = cfg['class']
            if class_name not in class_to_factors:
                class_to_factors[class_name] = []
            class_to_factors[class_name].append(factor_name)

        for class_name, factors in class_to_factors.items():
            if class_name in self.fc.class_cfg and self.fc.class_cfg[class_name]:
                for func_name in self.fc.class_cfg[class_name]:
                    class_func_id = f"classFunc_{class_name}_{func_name}"
                    for factor_name in factors:
                        self.links.append({
                            "source": class_func_id,
                            "target": factor_name,
                            "lineStyle": {"color": "#fac858", "width": 1, "type": "dashed"}
                        })

    def _add_mid_func_links(self):
        """添加midFunc连接"""
        for factor_name, cfg in self.fc.factor_cfg.items():
            if cfg['dependency']['midFunc']:
                for mid_func in cfg['dependency']['midFunc']:
                    mid_func_id = f"midFunc_{mid_func}"
                    self.links.append({
                        "source": mid_func_id,
                        "target": factor_name,
                        "lineStyle": {"color": "#73c0de", "width": 1, "type": "dotted"}
                    })

    def _add_data_source_links(self):
        """添加数据源连接"""
        for factor_name, cfg in self.fc.factor_cfg.items():
            for data_path in cfg['dataPath']:
                data_id = f"data_{data_path}"
                self.links.append({
                    "source": data_id,
                    "target": factor_name,
                    "lineStyle": {"color": "#9a60b4", "width": 1, "type": "dashed"}
                })

    def create_complete_dag(self, output_file: str = "complete_factor_dag.html"):
        """创建完整的因子依赖图"""
        # 清空现有数据
        self.nodes.clear()
        self.links.clear()

        # 添加所有节点
        self._add_factor_nodes()
        self._add_class_func_nodes()
        self._add_mid_func_nodes()
        self._add_data_source_nodes()

        # 调试：打印节点信息
        print("=== 节点调试信息 ===")
        for node in self.nodes:
            category_idx = node.get("category", 0)
            category_name = self.categories[category_idx]["name"]
            print(f"节点: {node['name']}, 类别: {category_name}")

        # 添加所有连接
        self._add_factor_dependencies()
        self._add_class_func_links()
        self._add_mid_func_links()
        self._add_data_source_links()

        # 创建图表
        graph = (
            Graph(init_opts=opts.InitOpts(width="1800px", height="1000px"))
                .add(
                "",
                nodes=self.nodes,
                links=self.links,
                categories=self.categories,
                layout="force",
                is_rotate_label=True,
                linestyle_opts=opts.LineStyleOpts(curve=0.2),
                label_opts=opts.LabelOpts(
                    position="right",
                    formatter="{b}"  # 只显示节点名称
                ),
                repulsion=500,
                gravity=0.1,
                edge_length=100,
            )
                .set_global_opts(
                title_opts=opts.TitleOpts(title="因子依赖关系图", pos_left="center"),
                legend_opts=opts.LegendOpts(
                    orient="vertical",
                    pos_left="2%",
                    pos_top="10%"
                ),
                tooltip_opts=opts.TooltipOpts(trigger="item")
            )
        )

        graph.render(output_file)
        print(f"完整因子依赖图已生成: {output_file}")

    def create_simple_dag(self, output_file: str = "simple_factor_dag.html"):
        """创建简化版因子依赖图（仅因子间依赖）"""
        self.nodes.clear()
        self.links.clear()

        # 只添加因子节点
        self._add_factor_nodes()
        self._add_factor_dependencies()

        # 只使用日频和分钟频的categories
        simple_categories = [self.categories[0], self.categories[1]]

        graph = (
            Graph(init_opts=opts.InitOpts(width="1600px", height="900px"))
                .add(
                "",
                nodes=self.nodes,
                links=self.links,
                categories=simple_categories,
                layout="force",
                is_rotate_label=True,
                linestyle_opts=opts.LineStyleOpts(curve=0.2),
                label_opts=opts.LabelOpts(
                    position="right",
                    formatter="{b}"  # 只显示节点名称
                ),
                repulsion=400,
                gravity=0.2,
                edge_length=80,
            )
                .set_global_opts(
                title_opts=opts.TitleOpts(title="简化因子依赖图", pos_left="center"),
                legend_opts=opts.LegendOpts(
                    orient="vertical",
                    pos_left="2%",
                    pos_top="10%"
                )
            )
        )

        graph.render(output_file)
        print(f"简化因子依赖图已生成: {output_file}")

    def create_class_dag(self, class_name: str, output_file: str = None):
        """创建特定类别的因子依赖图"""
        if output_file is None:
            output_file = f"{class_name}_factor_dag.html"

        self.nodes.clear()
        self.links.clear()

        # 添加该类别的因子
        class_factors = []
        for factor_name, cfg in self.fc.factor_cfg.items():
            if cfg['class'] == class_name:
                class_factors.append(factor_name)
                freq = cfg["params"]["freq"]

                self.nodes.append({
                    "name": factor_name,
                    "symbolSize": self._get_node_size("factor"),
                    "symbol": self._get_node_symbol("factor"),
                    "category": self._get_node_category("factor", freq)
                })

        # 添加依赖关系
        for factor_name in class_factors:
            cfg = self.fc.factor_cfg[factor_name]
            deps = cfg['dependency']['factor']
            if deps:
                if isinstance(deps, str):
                    deps = [deps]
                for dep in deps:
                    if dep in self.fc.factor_cfg:
                        self.links.append({
                            "source": dep,
                            "target": factor_name,
                            "lineStyle": {"color": "#5470c6", "width": 2}
                        })

        # 使用对应的类别
        class_categories = []
        if any(self.fc.factor_cfg[f]['params']['freq'] == 'day' for f in class_factors):
            class_categories.append(self.categories[0])
        if any(self.fc.factor_cfg[f]['params']['freq'] == 'minute' for f in class_factors):
            class_categories.append(self.categories[1])

        graph = (
            Graph(init_opts=opts.InitOpts(width="1400px", height="800px"))
                .add(
                "",
                nodes=self.nodes,
                links=self.links,
                categories=class_categories,
                layout="force",
                is_rotate_label=True,
                linestyle_opts=opts.LineStyleOpts(curve=0.2),
                label_opts=opts.LabelOpts(
                    position="right",
                    formatter="{b}"
                ),
                repulsion=300,
                gravity=0.3,
            )
                .set_global_opts(
                title_opts=opts.TitleOpts(title=f"{class_name}类别因子依赖图", pos_left="center")
            )
        )

        graph.render(output_file)
        print(f"{class_name}类别因子依赖图已生成: {output_file}")

    def create_dashboard(self):
        """创建可视化仪表板"""
        # 完整依赖图
        self.create_complete_dag()

        # 简化依赖图
        self.create_simple_dag()

        # 各类别因子图
        classes = set(cfg['class'] for cfg in self.fc.factor_cfg.values())
        for class_name in classes:
            self.create_class_dag(class_name)

        print("所有可视化图表已生成完成！")


# 使用示例
def visualize_factor_system(factor_calculator: FactorCalculator):
    """完整的可视化示例"""
    visualizer = FactorVisualizer(factor_calculator)

    # 生成所有图表
    visualizer.create_dashboard()


# 在主程序中调用
if __name__ == "__main__":
    from func import classFunc, shioMidFunc, shioCalFunc, varCalFunc, coinCalFunc

    with open(r".\config\factor.json5", "r", encoding='utf-8') as f:
        factor_cfg = json5.load(f)
    with open(r".\config\indicator.json5", "r", encoding='utf-8') as f:
        indicator_cfg = json5.load(f)
    with open(r".\config\class.json5", "r", encoding='utf-8') as f:
        class_cfg = json5.load(f)

    session = ddb.session()
    session.connect("172.16.0.184", 8001, "maxim", "dyJmoc-tiznem-1figgu")

    F = FactorCalculator(session=session, config=config,
                         factor_cfg=factor_cfg,
                         indicator_cfg=indicator_cfg,
                         func_map=get_funcMapFromImport(classFunc, shioMidFunc, shioCalFunc, varCalFunc, coinCalFunc),
                         class_cfg=class_cfg)

    F.set_factorList(factor_list=list(factor_cfg.keys()))

    # 初始化但不运行计算，只用于可视化
    F.init_check()

    # 生成可视化
    visualize_factor_system(F)