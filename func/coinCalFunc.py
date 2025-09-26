"""
注: 固定格式必须传入self: FactorCalculator, factorName: str, feature:Dict[Optional] {factor_cfg中的对应属性}
"""
from Calculator import FactorCalculator
from typing import Dict

def get_interDayReturn(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """过去一天的隔夜收益率,今日open-昨日close"""
    openCol = "stockDayKBar_open"
    closeCol = "stockDayKBar_close"
    return f"""
    {self.dataObj} =  select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,
                             ({openCol}-prev({closeCol}))\prev({closeCol}) as {factorName} from {self.sourceObj} 
                             context by {self.symbolCol}; 
    {self.factorDict}["{factorName}"] = {self.dataObj}; // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_interDayReturn_avg20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"].copy();
    update {self.middleObj} set {factorName} = mavg({dependFactor},20) context by {self.symbolCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_interDayReturn_std20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"].copy();
    update {self.middleObj} set {factorName} = mstd({dependFactor},20) context by {self.symbolCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """