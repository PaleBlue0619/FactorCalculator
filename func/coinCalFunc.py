"""
注: 固定格式必须传入self: FactorCalculator, factorName: str, feature:Dict[Optional] {factor_cfg中的对应属性}
"""
from Calculator import FactorCalculator
from typing import Dict
from func.utilFunc import mstd,mavg,reverse

def get_interDayReturn(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """过去一天的隔夜收益率,今日open-昨日close"""
    openCol = "stockDayKBar_open"
    closeCol = "stockDayKBar_close"
    return f"""
    {self.dataObj} =  select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,
                             nullFill(({openCol}-prev({closeCol}))\prev({closeCol}),0.0) as {factorName} from {self.sourceObj} 
                             context by {self.symbolCol}; 
    {self.factorDict}["{factorName}"] = {self.dataObj}; // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_intraDayReturn(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """过去一天的日内收益率"""
    openCol = "stockDayKBar_open"
    closeCol = "stockDayKBar_close"
    return f"""
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,
                    nullFill((prev({closeCol})-prev({openCol}))\prev({closeCol}),0.0) as {factorName} from {self.sourceObj}
                    context by {self.symbolCol};
    {self.factorDict}["{factorName}"] = {self.dataObj}; 
    print("因子{factorName}计算完毕");
    """

def get_intraDayTurnoverRateDiff(self: FactorCalculator, factorName: str, feature: Dict, **args):
    turnoverRateCol = "stockBasic_turnoverRate"
    return f"""
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,
                     nullFill({turnoverRateCol}-prev({turnoverRateCol}),0.0) as {factorName}
                     from {self.sourceObj}
                    context by {self.symbolCol};
    {self.factorDict}["{factorName}"] = {self.dataObj}; 
    print("因子{factorName}计算完毕");
    """


def get_interDayReturn_avg20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return mavg(self, factorName, dependFactor, k=20)

def get_interDayReturn_std20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return mstd(self, factorName, dependFactor, k=20)

def get_interDayReturnReverse(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"]  # 依赖计算的因子
    return reverse(self, factorName, dependFactor)

def get_interDayReturnReverse_avg20(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return mavg(self, factorName, dependFactor, k=20)

def get_interDayReturnReverse_std20(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return mstd(self, factorName, dependFactor, k=20)


def get_intraDayReturn_avg20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return mavg(self, factorName, dependFactor, k=20)

def get_intraDayReturn_std20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return mstd(self, factorName, dependFactor, k=20)

def get_intraDayReturnReverse(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"]  # 依赖计算的因子
    return reverse(self, factorName, dependFactor)

def get_intraDayReturnReverse_avg20(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return mavg(self, factorName, dependFactor, k=20)

def get_intraDayReturnReverse_std20(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return mstd(self, factorName, dependFactor, k=20)


def get_intraDayTurnoverRateDiff_avg20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return mavg(self, factorName, dependFactor, k=20)

def get_intraDayTurnoverRateDiff_std20(self: FactorCalculator, factorName: str, feature: Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return mstd(self, factorName, dependFactor, k=20)

def get_intraDayTurnoverRateDiffReverse(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"]  # 依赖计算的因子
    return reverse(self, factorName, dependFactor)

def get_intraDayTurnoverRateDiffReverse_avg20(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return mavg(self, factorName, dependFactor, k=20)

def get_intraDayTurnoverRateDiffReverse_std20(self: FactorCalculator, factorName: str, feature:Dict, **args):
    dependFactor = feature["dependency"]["factor"][0]   # 依赖计算的因子
    return mstd(self, factorName, dependFactor, k=20)