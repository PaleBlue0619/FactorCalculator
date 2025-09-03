"""
classFunc: 也就是这个factorClass

"""


def shioDataPrepare(self: FactorCalculator) -> Dict:
    """潮汐因子数据准备函数"""
    DayKBar = self.indicator_cfg["stock"]["DayKBar"]["indicator"]
    volume = DayKBar["stockDayKBar_amount"]
    amount = DayKBar["stockDayKBar_volume"]

    return {"cmd": f"""
    update {self.sourceObj} set vwap = nullFill!({volume}/{amount},0);
    update {self.sourceObj} set mVol = msum({volume},9) context by {self.symbolCol}, {self.dateCol}
    update {self.sourceObj} set mVol = move(mVol,4) context by {self.symbolCol}, {self.dateCol}
    """, "var": ["vwap","mVol"]}