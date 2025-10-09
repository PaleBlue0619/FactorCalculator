from Calculator import FactorCalculator
from typing import Dict


def mstd(self: FactorCalculator, factorName: str, dependFactor: str, k: int):
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"].copy();
    update {self.middleObj} set {factorName} = mstd({dependFactor},{k}) context by {self.symbolCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    // 截面空缺值填充
    update {self.dataObj} set {factorName} = nullFill({factorName},avg({factorName})) context by {self.dateCol};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def mavg(self: FactorCalculator, factorName: str, dependFactor: str, k: int):
    return f"""
    {self.middleObj} = {self.factorDict}["{dependFactor}"].copy();
    update {self.middleObj} set {factorName} = mavg({dependFactor},{k}) context by {self.symbolCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    // 截面空缺值填充
    update {self.dataObj} set {factorName} = nullFill({factorName},avg({factorName})) context by {self.dateCol};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def reverse(self: FactorCalculator, factorName: str, dependFactor: list):
    dependFactor0, dependFactor1 = dependFactor[0], dependFactor[1]
    return f"""
    {self.middleObj} = lj({self.factorDict}["{dependFactor0}"].copy(), {self.factorDict}["{dependFactor1}"].copy(), `{self.symbolCol}`{self.dateCol});
    update {self.middleObj} set {factorName} = 0.0;
    update {self.middleObj} set {factorName} = -1.0 * {dependFactor0} where {dependFactor1}<avg({dependFactor1}) context by {self.dateCol};
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,{factorName} from {self.middleObj};
    // 截面空缺值填充
    update {self.dataObj} set {factorName} = nullFill({factorName},avg({factorName})) context by {self.dateCol};
    {self.factorDict}["{factorName}"] = {self.dataObj};  // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def umr(self: FactorCalculator, factorName: str, dependFactor: list, k:int):
    returnFactor, riskFactor = dependFactor[0], dependFactor[1]
    return f"""
    {self.middleObj} = lj({self.factorDict}["{returnFactor}"].copy(), {self.factorDict}["{riskFactor}"].copy(), `{self.symbolCol}`{self.dateCol});
    {self.dataObj} =  select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,
                        msum({riskFactor}*({returnFactor}), 10) as {factorName} 
                        from {self.middleObj}
                        context by {self.symbolCol}
    // 截面空缺值填充
    update {self.dataObj} set {factorName} = nullFill({factorName},avg({factorName})) context by {self.dateCol};
    {self.factorDict}["{factorName}"] = {self.dataObj}
    print("因子{factorName}计算完毕");    
    """
