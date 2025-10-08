from Calculator import FactorCalculator
from typing import Dict
from func.utilFunc import *

openCol = "stockDayKBar_open"
closeCol = "stockDayKBar_close"
highCol = "stockDayKBar_high"
lowCol = "stockDayKBar_low"
turnoverRateCol = "stockBasic_turnoverRate"
idxPctChgCol = "stockDayIndex_pctChg"  # 指数收益率

def get_dayOverBenchRet(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """日内收益率"""
    return rf"""
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor, 
                    nullFill(({closeCol}-{openCol})\{openCol}-{idxPctChgCol},0.0) as {factorName}
                    from {self.sourceObj}
                    context by {self.symbolCol}
    {self.factorDict}["{factorName}"] = {self.dataObj}; 
    print("因子{factorName}计算完毕");
    """

def get_riskTR(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """日度TR真实波动"""
    return f"""
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,
                     byRow(max, [{highCol}-{lowCol}, abs({highCol}-prev({closeCol})), abs({lowCol}-prev({closeCol}))])\prev({closeCol}) as {factorName} 
                     from {self.sourceObj}
                     context by {self.symbolCol};
    // 截面空缺值均值填充
    update {self.dataObj} set {factorName} = nullFill({factorName}, avg({factorName})) context by {self.dateCol};
    {self.factorDict}["{factorName}"] = {self.dataObj}; 
    print("因子{factorName}计算完毕");
    """

def get_adjRiskTR10(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """调整后的日度TR真实波动"""
    dependFactor = feature["dependency"]["factor"][0]  # 依赖计算的因子
    return mavg(self, factorName, dependFactor, k=10)

def get_riskTurnoverRate(self: FactorCalculator, factorName: str, feature:Dict, **args):
    """日度换手率风险"""
    return f"""
    {self.dataObj} = select {self.symbolCol},{self.dateCol},"{factorName}" as `factor,
        {turnoverRateCol} as {factorName} from {self.sourceObj};
    {self.factorDict}["{factorName}"] = {self.dataObj};
    print("因子{factorName}计算完毕");
    """

def get_umrTR10(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """TR衡量的UMR"""
    return umr(self, factorName,
               dependFactor=feature["dependency"]["factor"],
               k=10)

def get_umrTurnoverRate10(self: FactorCalculator, factorName: str, feature: Dict, **args):
    """换手率衡量的UMR"""
    return umr(self, factorName,
               dependFactor=feature["dependency"]["factor"],
               k=10)
