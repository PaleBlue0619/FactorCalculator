"""
classFunc: 也就是这个factorClass

"""
from typing import Dict
from Calculator import FactorCalculator

def shioDataPrepare(self: FactorCalculator) -> Dict:
    """潮汐因子数据准备函数"""
    volumeCol = "stockMin1KBar_volume"
    amountCol = "stockMin1KBar_amount"

    return {"cmd": f"""
    update {self.sourceObj} set vwap = nullFill!({amountCol}/{volumeCol},0);
    update {self.sourceObj} set mVol = msum({volumeCol},9) context by {self.symbolCol}, {self.dateCol}
    update {self.sourceObj} set mVol = move(mVol,4) context by {self.symbolCol}, {self.dateCol}
    """, "columns": ["vwap","mVol"], "vars":None}

def vaRDataPrepare(self: FactorCalculator) -> Dict:
    """VaR因子数据准备函数"""
    closeCol = "stockMin1KBar_close"
    volumeCol = "stockMin1KBar_volume"
    amountCol = "stockMin1KBar_amount"
    return {"cmd": f"""
    varFunc = valueAtRisk{{,'normal',0.95}};
    cvarFunc = condValueAtRisk{{, 'normal',0.95}};
    update {self.sourceObj} set vwap = nullFill!({amountCol}/{volumeCol},0);
    update {self.sourceObj} set ret240 = nullFill(({closeCol}-move({closeCol},240))/{closeCol},0.0) context by {self.symbolCol};
    """}