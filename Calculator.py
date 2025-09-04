import os
import dolphindb as ddb
from typing import Dict, List
import json, json5

# 这里是主函数中设置的变量名称
config = {
    "dayFactorDB":"dfs://Dayfactor",
    "dayFactorTB":"pt",
    "minFactorDB":"dfs://Minfactor",
    "minFactorTB":"pt",
    "sourceObj": "df",  # 原始所需要指标的left join得到内存表的名称
    "middleObj": "midDict",  # 中间变量名称,字典格式,取的时候直接从字典取
    "dataObj": "data",  # 最终返回的因子变量名称
    "factorObj": "factorDict",  # 分钟频/日频共用因子Dict(不可被undef!)
    "symbolCol": "symbol",
    "dateCol": "TradeDate",
    "timeCol": "TradeTime",
    "minuteCol": "minute",
}

def complete_factor_cfg(factor_cfg: Dict) -> Dict:
    """
    使用迭代方式为每个因子补全dataPath信息
    收集所有底层依赖的dataPath并合并到当前因子的dataPath中
    """
    factor_map = {name: cfg.copy() for name, cfg in factor_cfg.items()}

    # 构建依赖关系
    dependency_map = {}
    for factor_name, cfg in factor_map.items():
        deps = cfg['dependency']['factor']
        if deps is None:
            deps = []
        elif isinstance(deps, str):
            deps = [deps]
        dependency_map[factor_name] = deps

    # 为每个因子找到所有底层依赖的dataPath
    for factor_name in factor_map:
        cfg = factor_map[factor_name]

        # 使用集合来跟踪访问过的因子，避免循环依赖
        visited = set()
        current_deps = dependency_map[factor_name][:]
        all_data_paths = []

        # 如果当前因子自己有dataPath，先加入
        if cfg['dataPath']:
            all_data_paths.extend(cfg['dataPath'])

        while current_deps:
            dep_factor = current_deps.pop(0)

            # 检查循环依赖
            if dep_factor in visited:
                raise ValueError(f"检测到循环依赖: {factor_name} -> {dep_factor}")
            visited.add(dep_factor)

            dep_cfg = factor_map[dep_factor]

            # 如果依赖的因子有dataPath，直接收集
            if dep_cfg['dataPath']:
                all_data_paths.extend(dep_cfg['dataPath'])

            # 无论依赖的因子是否有dataPath，都要继续追溯它的依赖
            # 因为可能依赖的因子本身没有dataPath，但它依赖的其他因子有
            current_deps.extend(dependency_map[dep_factor])

        # 去重并保持顺序
        unique_paths = []
        seen_paths = set()
        for path in all_data_paths:
            if path not in seen_paths:
                seen_paths.add(path)
                unique_paths.append(path)

        cfg['dataPath'] = unique_paths

    return factor_map

class FactorCalculator:
    def __init__(self, session:ddb.session, config: Dict, factor_cfg: Dict, indicator_cfg: Dict, class_cfg: Dict = None, factor_list: List = None):
        """初始化"""
        self.session = session
        self.dolphindb_cmd = "" # 最终合成的DolphinDB命令
        self.factor_list = factor_list       # 实际需要添加的因子列表
        self.factor_day_list = []   # 日频因子列表
        self.factor_min_list = []   # 分钟频因子列表
        self.class_dict = {}        # 所有因子对应的class以及对应的函数列表
        self.factorFunc_dict = {}

        self.config = config
        self.factor_cfg = factor_cfg
        self.indicator_cfg = indicator_cfg
        self.class_cfg = class_cfg
        self.dayDB = config["dayFactorDB"]   # 因子库名(日频)
        self.dayTB = config["dayFactorTB"]  # 因子表名(日频)
        self.minDB = config["minFactorDB"]
        self.minTB = config["minFactorTB"]
        self.sourceObj = config["sourceObj"]
        self.middleObj = config["middleObj"]
        self.dataObj = config["dataObj"]
        self.factorObj = config["factorObj"]
        self.symbolCol = config["symbolCol"]
        self.dateCol = config["dateCol"]
        self.timeCol = config["timeCol"]
        self.minuteCol = config["minute"]


    def set_factorList(self, factor_list: List):
        self.factor_list = factor_list

    def init_database(self, dropDayDB: bool = False, dropDayTB: bool = False, dropMinDB: bool = False, dropMinTB: bool = False):
        """创建DataBase"""
        if dropDayDB:
            if self.session.existsDatabase(dbUrl=self.dayDB):
                self.session.dropDatabase(dbPath=self.dayDB)
        if dropDayTB:
            if self.session.existsTable(dbUrl=self.dayDB,tableName=self.dayTB):
                self.session.dropTable(dbPath=self.dayDB,tableName=self.dayTB)
        if dropMinDB:
            if self.session.existsDatabase(dbUrl=self.minDB):
                self.session.dropDatabase(dbPath=self.minDB)
        if dropMinTB:
            if self.session.existsTable(dbUrl=self.minDB,tableName=self.minTB):
                self.session.dropTable(dbPath=self.minDB,tableName=self.minTB)
        if not self.session.existsTable(dbUrl=self.dayDB,tableName=self.dayTB):
            self.session.run(f"""
            db1 = database(,VALUE,2010.01M..2030.01M)
            db2 = database(,LIST,[`Maxim`DolphinDB])
            db = database({self.dayDB},partitionType=COMPO, partitionScheme=[db1, db2], engine="TSDB")
            schemaTb = table(1:0, ["symbol","date","factor","value"],[SYMBOL,DATE,SYMBOL,DOUBLE])
            db.createPartitionTable(schemaTb, {self.dayTB}, partitionColumns=`date`factor, sortColumns=`factor`symbol`date, keepDuplicates=LAST)
            """)
        if not self.session.existsTable(dbUrl=self.minDB,tableName=self.dayTB):
            self.session.run(f"""
            db1 = database(,VALUE,2010.01M..2030.01M)
            db2 = database(,LIST,[`Maxim`DolphinDB])
            db = database({self.minDB},partitionType=COMPO, partitionScheme=[db1, db2], engine="TSDB")
            schemaTb = table(1:0, ["symbol","date","time","factor","value"], [SYMBOL,DATE,TIME,SYMBOL,DOUBLE])
            db.createPartitionTable(schemaTb, {self.minTB}, partitionColumns=`date`factor, sortColumns=`factor`symbol`time`date, keepDuplicates=LAST)
            """)

    def init_check(self):
        """
        检查给定的config内部结构是否合理
        补全给定的config内部配置项信息
        """
        # 补全indicator_cfg中的indicator信息
        for assetType,Dict in self.indicator_cfg.items():
            for dbName, indicator_Dict in Dict.items():
                indicator_cfg = indicator_Dict["indicator"]
                for indicator in list(indicator_cfg.keys()):
                    if str(assetType)+str(dbName) not in indicator:
                        new_indicator = str(assetType)+str(dbName)+"_"+indicator
                        value = indicator_cfg[indicator]
                        self.indicator_cfg[assetType][dbName]["indicator"][new_indicator] = value   # 创建补全资产类别+数据库_指标名称的新指标名
                        self.indicator_cfg[assetType][dbName]["indicator"].pop(indicator)   # 删除老的键

        # 检查factor_cfg中的factor信息是否符合预期
        for factorName,Dict in self.factor_cfg.items():
            # 添加至对应频率的因子列表
            if str(Dict["params"]["freq"]).lower() in ["minute","m","min"]:
                self.factor_min_list.append(factorName)
            else:
                self.factor_day_list.append(factorName)
            # 添加至对应的class名-因子名Dict
            className = Dict["class"]
            if className not in self.class_dict.keys():
                self.class_dict[className] = []
            self.class_dict[className].append(factorName)

            # 添加至对应的函数名-因子名Dict
            funcName = Dict["calFunc"]
            self.factorFunc_dict[funcName] = factorName  # 函数名:因子名称, 只需要这个关系就行, 后续函数里面拿出来之后直接去原始factor_cfg中查剩下的属性即可

            # 记录这个因子所在的dataPath信息

        # 补全factor_cfg中的配置项信息
        self.factor_cfg = complete_factor_cfg(self.factor_cfg)

    def init_plan(self):
        """
        生成执行计划
        """
        return """"""

    def init_def(self):
        self.session.run(f"""
        {self.sourceObj}=0;
        {self.middleObj}=dict(STRING,ANY);  // 中间无关变量
        {self.dataObj}=0;
        {self.factorObj}=dict(STRING,ANY); // 因子变量,算完了丢进去
        """+self.data_insert()  # 定义插入函数
        )
    def data_insert(self):
        # 分批添加至数据库
        return f"""
        def InsertData(DBName, TBName, data, batchsize){{
            // 预防Out of Memory，分批插入数据，batchsize为每次数据的记录数
            start_idx = 0
            end_idx = batchsize
            krow = rows(data)
            do{{ 
                slice_data = data[start_idx:min(end_idx,krow),]
                if (rows(slice_data)>0){{
                loadTable(DBName, TBName).append!(slice_data);
                print(start_idx);
                }}
                start_idx = start_idx + batchsize
                end_idx = end_idx + batchsize
            }}while(start_idx < krow)
        }};
        InsertDayFactor = InsertData{{"insertDayDB", "insertDayTB", , }};
        InsertMinFactor = InsertData{{"insertMinDB", "insertMinTB", , }};
        """.replace("insertDayDB",self.dayDB).replace("insertDayTB",self.dayTB).replace("insertMinDB",self.minDB).replace("insertMinTB",self.minTB)


    def last_add(self, symbolCol:str, dateCol:str, nDays:int=1, marketType:str=None):
        """
        追加数据 SQL 生成
        """
        if not marketType:
            marketType = "XSHG"
        return f"""
        // 新增最新一个交易日的frame[用于日频决策]
        last_data=select * from data context by {symbolCol} limit -1; // 选取最后一个日期加一的形式作为决策的形式
        update last_data set {dateCol} = temporalAdd({dateCol},{nDays},"{marketType}")
        append!(pt,last_pt);
        undef(`last_pt);
        """

    def run(self, dropDayDB: bool = False, dropDayTB: bool = False, dropMinDB: bool = False, dropMinTB: bool = False):
        """主函数"""
        self.init_def() # 初始化相关变量+数据插入函数
        self.init_database(dropDayDB, dropDayTB, dropMinDB, dropMinTB)  # 初始化数据库
        self.init_check()


if __name__ == "__main__":
    with open(r".\config\factor.json5", "r",encoding='utf-8') as f:
        factor_cfg = json5.load(f)
    with open(r".\config\indicator.json5","r",encoding='utf-8') as f:
        indicator_cfg = json5.load(f)
    with open(r".\config\class.json5","r",encoding='utf-8') as f:
        class_cfg = json5.load(f)
    session=ddb.session()
    session.connect("172.16.0.184",8001,"maxim","dyJmoc-tiznem-1figgu")
    pool=ddb.DBConnectionPool("172.16.0.184",8001,10,"maxim","dyJmoc-tiznem-1figgu")
    F = FactorCalculator(session=session, config=config,
                         factor_cfg=factor_cfg,
                         indicator_cfg=indicator_cfg,
                         class_cfg=class_cfg)
    print(F.factor_cfg)