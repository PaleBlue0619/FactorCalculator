import os
import dolphindb as ddb
from typing import Dict
import json, json5

# 这里是主函数中设置的变量名称
config = {
    "sourceObj": "df",  # 原始所需要指标的left join得到内存表的名称
    "middleObj": "pt",  # 中间变量名称,字典格式,取的时候直接从字典取
    "dataObj": "data",  # 最终返回的因子变量名称
    "symbolCol": "symbol",
    "dateCol": "TradeDate",
    "timeCol": "TradeTime",
    "minuteCol": "minute",
}

class FactorCalculator:
    def __init__(self, config: Dict, factor_cfg: Dict, indicator_cfg: Dict):
        """初始化"""
        self.factor_cfg = factor_cfg
        self.indicator_cfg = indicator_cfg
        self.dolphindb_cmd = "" # 最终合成的DolphinDB命令
        self.sourceObj = config["sourceObj"]
        self.middleObj = config["middleObj"]
        self.dataObj = config["dataObj"]
        self.factorObj = config["factorObj"]
        self.symbolCol = config["dateCol"]
        self.timeCol = config["timeCol"]
        self.minuteCol = config["minute"]
        self.class_dict = {}        # 所有因子对应的class以及对应的函数列表
        self.factor_list = []       # 实际所需的因子名称列表
        self.factor_list_all = []   # 所有需要纳入计算的因子名称列表

    def setFuncList(self):

    def init_check(self):
        """
        检查给定的config内部结构是否合理
        """
        return """"""

    def init_plan(self):
        """
        生成执行计划
        """
    def init_fill(self):
        """
        根据执行计划补全配置项
        """


    def init_def(self,session: ddb.session):
        session.run(f"""
        {self.sourceObj}=0;
        {self.middleObj}=dict(STRING,ANY);
        {self.factorObj}=0;
        """)

    def left_join(self):
        """
        left join SQL生成
        """
        return """"""

    def last_add(self, symbolCol:str, dateCol:str, nDays:int=1, marketType:str=None):
        """
        追加数据 SQL 生成
        """
        if not marketType:
            marketType = "XSHG"
        return f"""
        // 新增最新一个交易日的frame[用于日频决策]
        last_data=select * from data context by {symbolCol} limit -1; // 选取最后一个日期加一的形式作为决策的形式
        update last_data set {dateCol} = temporalAdd({dateCol},{nDays},"{marketType}")
        append!(pt,last_pt);
        undef(`last_pt);
        """

    def batch_cal(self):
        """
        批量计算
        """

    def data_insert(self):
        # 分批添加至数据库
        return f"""
        def InsertData(DBName, TBName, data, batchsize){{
            // 预防Out of Memory，分批插入数据，batchsize为每次数据的记录数
            start_idx = 0
            end_idx = batchsize
            krow = rows(data)
            do{{ 
                slice_data = data[start_idx:min(end_idx,krow),]
                if (rows(slice_data)>0){{
                loadTable(DBName, TBName).append!(slice_data);
                print(start_idx);
                }}
                start_idx = start_idx + batchsize
                end_idx = end_idx + batchsize
            }}while(start_idx < krow)
        }};
        """



if __name__ == "__main__":
    with open(r".\factor\expr\factor.json5", "r",encoding='utf-8') as f:
        factor_cfg = json5.load(f)
    with open(r".\factor\indicator\indicator.json5","r",encoding='utf-8') as f:
        indicator_cfg = json5.load(f)
    F = FactorCalculator(factor_cfg=factor_cfg, indicator_cfg=indicator_cfg)
    print(factor_cfg)