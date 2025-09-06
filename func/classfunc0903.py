"""
classFunc: 也就是这个factorClass

"""
from typing import Dict
from Calculator import FactorCalculator

def shioDataPrepare(self: FactorCalculator) -> Dict:
    """潮汐因子数据准备函数"""
    volume = "stockMin1KBar_volume"
    amount = "stockMin1KBar_amount"

    return {"cmd": f"""
    update {self.sourceObj} set vwap = nullFill!({amount}/{volume},0);
    update {self.sourceObj} set mVol = msum({volume},9) context by {self.symbolCol}, {self.dateCol}
    update {self.sourceObj} set mVol = move(mVol,4) context by {self.symbolCol}, {self.dateCol}
    """, "columns": ["vwap","mVol"], "vars":None}