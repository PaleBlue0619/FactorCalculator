import os
from Calculator import FactorCalculator
from typing import Dict

def shioFunc(self: FactorCalculator) -> Dict:
    # 潮汐因子计算函数
    return {"cmd": f"""
    defg shioFunc(mvol, price){{ // defg 聚合函数声明
        idx_max = imax(mvol);
        priceList_m = price[:idx_max]
        idx_m = imin(priceList_m) 
        priceList_n = price[idx_max+1:]
        idx_n = imin(priceList_n)
        Cm = price[idx_m]
        Cn = price[idx_n]
        res = (Cn-Cm)\(Cm)\(idx_n-idx_m)
        return res
    }};
    """, "var": None}

def shioStrongFunc(self: FactorCalculator) -> Dict:
    # 涨潮半潮汐计算函数
    return {"cmd":f"""
    defg shioStrongFunc(mvol, price){{ // defg聚合函数声明
        idx_max = imax(mvol);
        Cmax = price[idx_max];
        priceList_m = price[:idx_max]
        idx_m = imin(priceList_m) 
        priceList_n = price[idx_max+1:]
        idx_n = imin(priceList_n)
        Cm = price[idx_m]
        Cn = price[idx_n]
        Vm = mvol[idx_m]
        Vn = mvol[idx_n]
        res = iif(Vm<Vn, (Cmax-Cm)\Cm\(idx_max-idx_m), (Cn-Cmax)\Cmax\(idx_n-idx_max))
        return res
        }};
    ""","var":None}

def shioWeakFunc(self: FactorCalculator) -> Dict:
    # 退潮半潮汐函数
    return {"cmd":f"""
     defg shioWeakFunc(mvol, price){{ // defg聚合函数声明
        idx_max = imax(mvol);
        Cmax = price[idx_max];
        priceList_m = price[:idx_max]
        idx_m = imin(priceList_m) 
        priceList_n = price[idx_max+1:]
        idx_n = imin(priceList_n)
        Cm = price[idx_m]
        Cn = price[idx_n]
        Vm = mvol[idx_m]
        Vn = mvol[idx_n]
        res = iif(Vm>Vn, (Cmax-Cm)\Cm\(idx_max-idx_m), (Cn-Cmax)\Cmax\(idx_n-idx_max))
        return res
    }};
    ""","var":None}


