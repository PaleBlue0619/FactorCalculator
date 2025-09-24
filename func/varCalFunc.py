"""
注: 固定格式必须传入self: FactorCalculator, factorName: str, feature:Dict[Optional] {factor_cfg中的对应属性}
"""
from Calculator import FactorCalculator
from typing import Dict

def get_vaR240_m120(self: FactorCalculator, factorName: str, feature: Dict, **args):
    return f"""
    // dateCol,symbolCol,factorName
    {self.dataObj} = select {self.symbolCol},{self.dateCol},
                    "{factorName}" as `factor,
                    moving(varFunc,ret240,120,1) as `{factorName} 
                    from {self.sourceObj}
                    context by symbol
                    order by symbol;
    {self.factorDict}["{factorName}"] = {self.dataObj}; // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """

def get_cvaR240_m120(self: FactorCalculator, factorName: str, feature: Dict, **args):
    return f"""
    // dateCol,symbolCol,factorName
    {self.dataObj} = select {self.symbolCol},{self.dateCol},
                    "{factorName}" as `factor,
                    moving(cvarFunc,ret240,120,1) as `{factorName} 
                    from {self.sourceObj}
                    context by symbol
                    order by symbol;
    {self.factorDict}["{factorName}"] = {self.dataObj}; // 丢进因子数据变量
    print("因子{factorName}计算完毕");
    """