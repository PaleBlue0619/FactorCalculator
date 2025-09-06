"""
注: 固定格式必须传入self: FactorCalculator, feature: str, feature:Dict {factor_cfg中的对应属性}
"""
from Calculator import FactorCalculator
from typing import Dict

def get_shio(self: FactorCalculator, factorName: str, feature: Dict, **args):
    closeCol = "stockMin1KBar_close"
    return f"""
    {self.middleObj} = select shioFunc(mVol, {closeCol}) as {factorName} from {self.sourceObj} 
                        group by {self.dateCol}, {self.symbolCol} order by {self.dateCol};
    // dateCol,symbolCol,factorName
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj}; // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_shio_avg20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"];
    update {self.middleObj} set {factorName} = mavg({dependFactor},20) context by {self.symbolCol},{self.dateCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_shio_std20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"];
    update {self.middleObj} set {factorName} = mstd({dependFactor},20) context by {self.symbolCol},{self.dateCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_shioStrong(self: FactorCalculator, factorName: str, **args):
    closeCol = "stockMin1KBar_close"
    return f"""
    {self.middleObj} = select shioStrongFunc(mVol, {closeCol}) as {factorName} from {self.sourceObj} 
                        group by {self.dateCol}, {self.symbolCol} order by {self.dateCol};
    // dateCol,symbolCol,factorName
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj}; // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_shioStrong_avg20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"];
    update {self.middleObj} set {factorName} = mavg({dependFactor},20) context by {self.symbolCol},{self.dateCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_shioStrong_std20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"];
    update {self.middleObj} set {factorName} = mstd({dependFactor},20) context by {self.symbolCol},{self.dateCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """