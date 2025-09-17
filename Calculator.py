import os
import pandas as pd
import networkx as nx
import dolphindb as ddb
from typing import Dict, List
import json, json5

# 这里是主函数中设置的变量名称
config = {
    "dayFactorDB":"dfs://Dayfactor",
    "dayFactorTB":"pt",
    "minFactorDB":"dfs://Minfactor",
    "minFactorTB":"pt",
    "sourceDict": "sourceDict", # 所有原始指标left join后存入的字典
    "sourceObj": "df",  # 原始所需要指标的left join得到内存表的名称
    "middleObj": "middle",  # 中间变量名称,字典格式,取的时候直接从字典取
    "dataObj": "data",  # 最终返回的因子变量名称
    "factorDict": "factorDict",  # 分钟频/日频共用因子Dict(不可被undef!)
    "symbolCol": "symbol",
    "dateCol": "TradeDate",
    "timeCol": "TradeTime",
    "minuteCol": "minute",
}

def trans_time(start_date: str, end_date:str):
    """转换日期格式为DolphinDB能够识别的格式"""
    # 限制因子计算时间
    if not start_date:
        start_date = "2020.01.01"
    if not end_date:
        end_date = pd.Timestamp(now).strftime("%Y.%m.%d")
    start_date = pd.Timestamp(start_date).strftime("%Y.%m.%d")
    end_date = pd.Timestamp(end_date).strftime("%Y.%m.%d")
    return start_date, end_date


def get_factor_byDependency(factor_cfg: Dict, factor_list: List[str]) -> List[str]:
    """
    安全版本：处理循环依赖
    """
    G = nx.DiGraph()

    # 构建依赖图
    for factor, cfg in factor_cfg.items():
        deps = cfg['dependency']['factor'] or []
        deps = [deps] if isinstance(deps, str) else deps
        for dep in deps:
            G.add_edge(dep, factor)

    # 收集所有相关节点
    all_nodes = set(factor_list)
    for factor in factor_list:
        all_nodes.update(nx.ancestors(G, factor))

    # 检查循环依赖
    try:
        return list(nx.topological_sort(G.subgraph(all_nodes)))
    except nx.NetworkXUnfeasible:
        # 有循环依赖时，返回按字母排序
        return sorted(all_nodes)

def get_funcMapFromImport(*modules):
    """从指定模块收集函数"""
    function_map = {}
    for module in modules:
        for name in dir(module):
            obj = getattr(module, name)
            if callable(obj):  # and not name.startswith('_'):
                function_map[name] = obj
    return function_map


def complete_factor_cfg(factor_cfg: Dict) -> Dict:
    """
    使用DFS方式为每个因子补全dataPath信息
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

    # 缓存已计算的结果，避免重复计算
    data_path_cache = {}

    def get_all_data_paths(factor_name, visited=None):
        if visited is None:
            visited = set()

        # 检查循环依赖
        if factor_name in visited:
            raise ValueError(f"检测到循环依赖: {' -> '.join(visited)} -> {factor_name}")

        # 如果已经缓存，直接返回
        if factor_name in data_path_cache:
            return data_path_cache[factor_name]

        visited.add(factor_name)
        cfg = factor_map[factor_name]

        # 收集所有依赖的dataPath
        all_data_paths = set()

        # 添加当前因子的dataPath
        if cfg['dataPath']:
            all_data_paths.update(cfg['dataPath'])

        # 递归处理所有依赖
        for dep in dependency_map[factor_name]:
            dep_paths = get_all_data_paths(dep, visited.copy())
            all_data_paths.update(dep_paths)

        # 转换为列表并保持顺序（如果需要）
        result = list(all_data_paths)
        data_path_cache[factor_name] = result
        return result

    # 为每个因子计算完整的dataPath
    for factor_name in factor_map:
        if factor_name not in data_path_cache:
            factor_map[factor_name]['dataPath'] = get_all_data_paths(factor_name)

    return factor_map

def sort_factor_byDependency(factor_cfg: Dict, factor_list: List[str]) -> List[str]:
    """
    简单的依赖关系排序
    """
    G = nx.DiGraph()
    G.add_nodes_from(factor_list)

    # 构建依赖图
    for factor in factor_list:
        if factor in factor_cfg:
            deps = factor_cfg[factor]['dependency']['factor']
            if deps is None:
                continue
            if isinstance(deps, str):
                deps = [deps]
            for dep in deps:
                if dep in factor_list:
                    G.add_edge(dep, factor)

    try:
        return list(nx.topological_sort(G))
    except nx.NetworkXUnfeasible:
        # 循环依赖时返回原始列表
        print("警告: 存在循环依赖，返回原始顺序")
        return factor_list


class FactorCalculator:
    def __init__(self, session: ddb.session,
                 config: Dict,
                 factor_cfg: Dict,
                 indicator_cfg: Dict,
                 func_map: Dict,    # 函数str:对应函数Obj
                 class_cfg: Dict = None,
                 ):

        """初始化"""
        self.session = session
        self.dolphindb_cmd = "" # 最终合成的DolphinDB命令
        self.factor_need = []       # 实际需要添加的因子列表
        self.factor_list = []       # 所有需要的因子列表

        # 划分维度1:
        self.factor_day_list = []   # 日频因子列表
        self.factor_min_list = []   # 分钟频因子列表

        # 划分维度2: 按照数据库维度进行划分->统一维度/不同维度
        self.dataPath_D_list = []   # 日频数据表
        self.dataPath_M_list = []   # 分钟频数据表
        self.factor_DD_list = []    # 需要纯日频数据/日频+日频数据
        self.factor_MM_list = []    # 需要纯分钟频数据/分钟频+分钟频数据
        self.factor_MD_list = []    # 分钟频left join日频数据
        self.dataPath_DD_dict = {}
        self.dataPath_MM_dict = {}
        self.dataPath_MD_dict = {}
        self.classFactorName_dict = {}  # class: FactorSet
        self.factorFuncName_dict = {}

        self.config = config
        self.factor_cfg = factor_cfg
        self.indicator_cfg = indicator_cfg
        self.func_map = func_map
        self.class_cfg = class_cfg if class_cfg else {}
        self.dayDB = config["dayFactorDB"]   # 因子库名(日频)
        self.dayTB = config["dayFactorTB"]  # 因子表名(日频)
        self.minDB = config["minFactorDB"]
        self.minTB = config["minFactorTB"]
        self.sourceDict = config["sourceDict"]
        self.sourceObj = config["sourceObj"]
        self.middleObj = config["middleObj"]
        self.dataObj = config["dataObj"]
        self.factorDict = config["factorDict"]
        self.symbolCol = config["symbolCol"]
        self.dateCol = config["dateCol"]
        self.timeCol = config["timeCol"]
        self.minuteCol = config["minuteCol"]

    def set_factorList(self, factor_list: List):
        self.factor_need = factor_list  # 真正需要传上去的因子列表
        self.factor_list = get_factor_byDependency(factor_cfg=self.factor_cfg,
                                                   factor_list=factor_list)
        self.factor_cfg = {factor: self.factor_cfg[factor] for factor in self.factor_list}

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
            db2 = database(,VALUE,[`Maxim,`DolphinDB])
            db = database("{self.dayDB}",partitionType=COMPO, partitionScheme=[db1, db2], engine="TSDB")
            schemaTb = table(1:0, ["symbol","date","factor","value"],[SYMBOL,DATE,SYMBOL,DOUBLE])
            db.createPartitionedTable(schemaTb, "{self.dayTB}", partitionColumns=`date`factor, sortColumns=`factor`symbol`date, keepDuplicates=LAST)
            """)
        if not self.session.existsTable(dbUrl=self.minDB,tableName=self.dayTB):
            self.session.run(f"""
            db1 = database(,VALUE,2010.01M..2030.01M)
            db2 = database(,VALUE,[`Maxim,`DolphinDB])
            db = database("{self.minDB}",partitionType=COMPO, partitionScheme=[db1, db2], engine="TSDB")
            schemaTb = table(1:0, ["symbol","date","time","factor","value"], [SYMBOL,DATE,TIME,SYMBOL,DOUBLE])
            db.createPartitionedTable(schemaTb, "{self.minTB}", partitionColumns=`date`factor, sortColumns=`factor`symbol`time`date, keepDuplicates=LAST)
            """)

    def init_check(self):
        """
        检查给定的config内部结构是否合理
        补全给定的config内部配置项信息
        """
        # 补全indicator_cfg中的indicator信息
        for dbName, indicator_Dict in self.indicator_cfg.items():
            # 判断数据库表对应的时间频率
            if str(indicator_Dict["dataFreq"]).lower() in ["day","d","daily"]:
                self.dataPath_D_list.append(dbName)
            else:
                self.dataPath_M_list.append(dbName)

            # 补全字段名称
            indicator_cfg = indicator_Dict["indicator"]
            for indicator in list(indicator_cfg.keys()):
                if str(dbName) not in indicator:
                    new_indicator = str(assetType)+str(dbName)+"_"+indicator
                    value = indicator_cfg[indicator]
                    self.indicator_cfg[assetType][dbName]["indicator"][new_indicator] = value   # 创建补全资产类别+数据库_指标名称的新指标名
                    self.indicator_cfg[assetType][dbName]["indicator"].pop(indicator)   # 删除老的键

        # 检查factor_cfg中的factor信息是否符合预期
        for factorName,Dict in self.factor_cfg.items():
            # 补全indicator信息
            for i in range(len(Dict["dataPath"])):
                (dataPath, indicatorList) = Dict["dataPath"][i], Dict["indicator"][i]
                for j in range(len(indicatorList)): # 对当前数据库下的每一个indicator进行判断
                    if str(dataPath) not in indicatorList[j]:
                        self.factor_cfg[factorName]["indicator"][i][j] = str(dataPath)+"_"+self.factor_cfg[factorName]["indicator"][i][j]

            # 添加至对应频率的因子列表
            if str(Dict["params"]["freq"]).lower() in ["minute","m","min"]:
                self.factor_min_list.append(factorName)
            else:
                self.factor_day_list.append(factorName)
            # 添加至对应的class名-因子名Dict
            className = Dict["class"]
            if className not in self.classFactorName_dict.keys():
                self.classFactorName_dict[className] = []
            self.classFactorName_dict[className].append(factorName)

            # 添加至对应的函数名-因子名Dict
            funcName = Dict["calFunc"]
            self.factorFuncName_dict[funcName] = factorName  # 函数名:因子名称, 只需要这个关系就行, 后续函数里面拿出来之后直接去原始factor_cfg中查剩下的属性即可

        # 补全factor_cfg中的配置项信息
        self.factor_cfg = complete_factor_cfg(self.factor_cfg)
        for factorName,Dict in self.factor_cfg.items():
            # 根据所需数据库表时间频率判断因子∈DD型/MM型/DM型
            dataPath = Dict["dataPath"] # list
            relyDaily, relyMinute = False, False
            for path in dataPath:
                if path in self.dataPath_D_list:
                    relyDaily = True
                if path in self.dataPath_M_list:
                    relyMinute = True
            if relyDaily and not relyMinute:
                self.factor_DD_list.append(factorName)
            elif relyMinute and not relyDaily:
                self.factor_MM_list.append(factorName)
            else:
                self.factor_MD_list.append(factorName)
            # 填充DD_Dict, MD_Dict, MM_Dict
            self.dataPath_D_list

    def init_def(self):
        self.session.run(f"""
        {self.sourceObj}=0;
        {self.middleObj}=dict(STRING,ANY);  // 中间无关变量
        {self.dataObj}=0;
        {self.factorDict}=dict(STRING,ANY); // 因子变量,算完了丢进去
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

    def no_leftJoin(self, dataPath: str, indicator_dict: dict, start_date:str, end_date: str):
        start_date, end_date = trans_time(start_date, end_date)
        cfg = self.indicator_cfg[dataPath]
        for colName in ["symbolCol","dateCol","timeCol"]:
            if cfg[colName] in ["", None]:
                cfg[colName] = "NA"
        return f"""
         // 配置项
        start_date = {start_date};
        end_date = {end_date};
        dbName = "{cfg["dataPath"][0]}";
        tbName = "{cfg["dataPath"][1]}";
        symbolCol = "{cfg["symbolCol"]}";
        dateCol = "{cfg["dateCol"]}";
        timeCol = "{cfg["timeCol"]}";
        idxCols = array(STRING,0);
        matchingCols = array(STRING,0);
        indicator_dict = {indicator_dict};
        if (dateCol!="NA"){{
            idxCols.append!(ldateCol);
            matchingCols.append!("TradeDate");
        }};
        if (timeCol!="NA"){{
            idxCols.append!(timeCol);
            matchingCols.append!("TradeTime");
        }};
        if (symbolCol!="NA"){{
            idxCols.append!(symbolCol);
            matchingCols.append!("symbol");
        }};
        names = matchingCols.copy().append!(string(indicator_dict.keys()));
        selects = idxCols.copy().append!(string(indicator_dict.values()));
        if (dateCol!="NA"){{
            {self.sourceObj} = <select _$$selects as _$$names from loadTable(dbName, tbName) where _$dateCol between start_date and end_date>.eval()              
        }}else{{
            {self.sourceObj} = <select _$$selects as _$$names from loadTable(dbName, tbName)>.eval()          
        }}
        """

    def first_leftJoin(self, lpath: str, rpath: str, lindicator_dict: dict, rindicator_dict:dict, start_date: str, end_date: str):
        """
        生成left join语句(第一次生成SourceObj的语句, 后续由left_join函数使得SourceObj增量去left join右表)
        """
        start_date, end_date = trans_time(start_date, end_date)
        # 获取左右表的数据库表名称
        lcfg = self.indicator_cfg[lpath]
        rcfg = self.indicator_cfg[rpath]
        for colName in ["symbolCol","dateCol","timeCol"]:
            if lcfg[colName] in ["", None]:
                lcfg[colName] = "NA"
            if rcfg[colName] in ["", None]:
                rcfg[colName] = "NA"

        return f"""
        // 第一次left join
        // 配置项
        start_date = {start_date};
        end_date = {end_date};
        ldbName = "{lcfg["dataPath"][0]}";
        ltbName = "{lcfg["dataPath"][1]}";
        rdbName = "{rcfg["dataPath"][0]}";
        rtbName = "{rcfg["dataPath"][1]}";
        lsymbolCol = "{lcfg["symbolCol"]}";
        ldateCol = "{lcfg["dateCol"]}";
        ltimeCol = "{lcfg["timeCol"]}";
        rsymbolCol = "{rcfg["symbolCol"]}";
        rdateCol = "{rcfg["dateCol"]}";
        rtimeCol = "{rcfg["timeCol"]}";
        // 首先确定matchingCols ->左右表均不为NA的列名
        leftIdxCols = array(STRING,0);
        rightIdxCols = array(STRING,0);
        matchingCols = array(STRING,0);
        if (ldateCol!="NA" and rdateCol!="NA"){{
            leftIdxCols.append!(ldateCol);
            rightIdxCols.append!(rdateCol);
            matchingCols.append!("TradeDate");
        }};
        if (ltimeCol!="NA" and rtimeCol!="NA"){{
            leftIdxCols.append!(ltimeCol);
            rightIdxCols.append!(rtimeCol);
            matchingCols.append!("TradeTime");
        }};
        if (lsymbolCol!="NA" and rsymbolCol!="NA"){{
            leftIdxCols.append!(lsymbolCol);
            rightIdxCols.append!(rsymbolCol);
            matchingCols.append!("symbol");
        }};
        lindicator_dict = {lindicator_dict};
        rindicator_dict = {rindicator_dict};
        lnames = matchingCols.copy().append!(string(lindicator_dict.keys()));
        lselects = leftIdxCols.copy().append!(string(lindicator_dict.values()));
        rnames = matchingCols.copy().append!(string(rindicator_dict.keys()));
        rselects = rightIdxCols.copy().append!(string(rindicator_dict.values()));

        if (ldateCol!="NA"){{
            leftTable = <select _$$lselects as _$$lnames from loadTable(ldbName, ltbName) where _$ldateCol between start_date and end_date>.eval()              
        }}else{{
            leftTable = <select _$$lselects as _$$lnames from loadTable(ldbName, ltbName)>.eval()          
        }}
        if (rdateCol!="NA"){{
            rightTable = <select _$$rselects as _$$rnames from loadTable(rdbName, rtbName) where _$rdateCol between start_date and end_date>.eval()              
        }}else{{
            rightTable = <select _$$rselects as _$$rnames from loadTable(rdbName, rtbName)>.eval()          
        }};        
        {self.sourceObj} = lsj(leftTable, rightTable, matchingCols);     
        """

    def after_leftJoin(self, rpath: str, rindicator_dict: dict, start_date: str, end_date: str):
        start_date, end_date = trans_time(start_date, end_date)
        # 左表的名称为$sourceObj获取右表的数据库名称
        rcfg = self.indicator_cfg[rpath]
        for colName in ["symbolCol", "dateCol", "timeCol"]:
            if rcfg[colName] in ["", None]:
                rcfg[colName] = "NA"
        return f"""
        // 第二次及后续left join
        // 配置项
        start_date = {start_date};
        end_date = {end_date};
        rdbName = "{rcfg["dataPath"][0]}"
        rtbName = "{rcfg["dataPath"][1]}"
        rsymbolCol = "{rcfg["symbolCol"]};
        rdateCol = "{rcfg["dateCol"]};
        rtimeCol = "{rcfg["timeCol"]};
        rightIdxCols = array(STRING,0);
        matchingCols = array(STRING,0)
        rindicator_dict = {rindicator_dict}
        // 这里的leftObj都是第一次Left semi join中的左表
        if (ldateCol!="NA" and rdateCol!="NA"){{  
            rightIdxCols.append!(rdateCol);
            matchingCols.append!("TradeDate");
        }};
        if (ltimeCol!="NA" and rtimeCol!="NA"){{
            rightIdxCols.append!(rtimeCol);
            matchingCols.append!("TradeTime");
        }};
        if (lsymbolCol!="NA" and rsymbolCol!="NA"){{
            rightIdxCols.append!(rsymbolCol);
            matchingCols.append!("symbol");
        }};
        rnames = matchingCols.copy().append!(string(rindicator_dict.keys()))
        rselects = rightIdxCols.copy().append!(string(rindicator_dict.values()))
        
        if (rdateCol!="NA"){{
            rightTable = <select _$$rselects as _$$rnames from loadTable(rdbName, rtbName) where _$rdateCol between start_date and end_date>.eval()              
        }}else{{
            rightTable = <select _$$rselects as _$$rnames from loadTable(rdbName, rtbName)>.eval()          
        }}
        {self.sourceObj} = lsj({self.sourceObj}, rightTable, matchingCols);
        """

    def last_add(self, symbolCol:str, dateCol:str, nDays: int = 1, marketType: str = None):
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

    def update_data(self):
        """
        最后上传所有所需数据至指定数据库
        """
        day_factor_need= [i for i in self.factor_day_list if i in self.factor_need]
        min_factor_need= [i for i in self.factor_min_list if i in self.factor_need]
        return f"""            
            day_factor_need = {day_factor_need};  // 所有需要添加至日频因子数据库的因子列表
            min_factor_need = {min_factor_need};  // 所有需要添加至分钟频因子数据库的因子列表
            // 先向数据库添加指定分区
            if (size(day_factor_need)>0){{
                addValuePartitions(database("{self.dayDB}"),day_factor_need,1); // 添加至COMPO分区的第一层
            }}
            if (size(min_factor_need)>0){{
                addValuePartitions(database("{self.minDB}"),min_factor_need,1); // 添加至COMPO分区的第一层
            }}            
            for (factor in day_factor_need){{
                print(select * from {self.factorDict}[factor] limit 10)
                InsertDayFactor({self.factorDict}[factor],1000000); 
                print("日频因子"+factor+"Insert完毕");
            }};
            for (factor in min_factor_need){{
                InsertMinFactor({self.factorDict}[factor],1000000);
                print("分钟频因子"+factor+"Insert完毕");
            }}
        """


    def get_featuresGivenFactor(self, factor_list: List) -> Dict:
        """
        给定因子list, 自动返回一个Dict<dataPath: feature_Dict>
        注: init_check后补全相关配置+规范相关特征名称后再运行
        """
        resDict = {}
        for factor in factor_list:
            dataPathList = self.factor_cfg[factor]["dataPath"]
            indicatorList = self.factor_cfg[factor]["indicator"]
            for dataPath, indicators in zip(dataPathList, indicatorList):
                if dataPath not in resDict:
                    resDict[dataPath] = {ind: self.indicator_cfg[dataPath]["indicator"][ind] for ind in indicators}
                else:
                    resDict[dataPath].update({ind: self.indicator_cfg[dataPath]["indicator"][ind] for ind in indicators})
        return resDict

    def sort_factorsGivenDependency(self, factor_list: List) -> List:
        """
        [核心函数]
        1. 根据配置项中因子的依赖关系对因子执行顺序进行编排
        2. [Optional说实在]同一依赖关系分支中, 再按照因子类进行排序
        """
        return sort_factor_byDependency(factor_cfg=self.factor_cfg,
                                      factor_list=factor_list)


    def run(self, start_date: str, end_date: str,
            dropDayDB: bool = False,
            dropDayTB: bool = False,
            dropMinDB: bool = False,
            dropMinTB: bool = False):
        """主函数"""
        start_date, end_date = trans_time(start_date, end_date)
        # Step1. 初始化
        self.init_def() # 初始化相关变量+数据插入函数
        self.init_database(dropDayDB, dropDayTB, dropMinDB, dropMinTB)  # 初始化数据库
        self.init_check()

        # Step2. DD_list/MM_list/MD_list
        # MD_list (left-join)
        self.dataPath_MD_dict = {} # 存储所有MD格式的dataPath以及对应的因子list
        for factorName in self.factor_MD_list:  # 遍历所有MD类型的因子
            dataPath = "$".join(self.factor_cfg[factorName]["dataPath"])
            if dataPath not in self.dataPath_MD_dict.keys():
                self.dataPath_MD_dict[dataPath] = []
            self.dataPath_MD_dict[dataPath].append(factorName)
        for dataPath,factorList in self.dataPath_MD_dict.items():    # 遍历所有MD类型的数据库以及对应的因子List
            # 准备这个dataPath下需要哪些特征 -> dict(dbName, feature_dict)
            dataPath = dataPath.split("$")
            featureDict = self.get_featuresGivenFactor(factorList)
            # 获取这个dataPath下有那些class的因子
            classList = list(set([self.factor_cfg[factor]["class"] for factor in factorList]))

            # 先添加left join数据 -> 对于该数据库对+对应的因子列表，初始化sourceObj
            if len(dataPath) == 1:  # 说明不需要执行semi-leftJoin
                self.dolphindb_cmd += self.no_leftJoin(dataPath=dataPath[0],
                                                     indicator_dict=featureDict[dataPath[0]],
                                                     start_date=start_date,
                                                     end_date=end_date)
            elif len(dataPath) >= 2: # 说明需要执行semi-leftJoin
                self.dolphindb_cmd += self.first_leftJoin(lpath=dataPath[0], rpath=dataPath[1],
                                                          lindicator_dict=featureDict[dataPath[0]],
                                                          rindicator_dict=featureDict[dataPath[1]],
                                                          start_date=start_date,
                                                          end_date=end_date)
                if len(dataPath) >= 3: # 说明从左到右依次执行多次semi-leftJoin
                    for i in range(2,len(dataPath)):
                        self.dolphindb_cmd += self.after_leftJoin(rpath=dataPath[i],
                                                                  rindicator_cfg=featureDict[dataPath[i]],
                                                                  start_date=start_date,
                                                                  end_date=end_date)
            # 将factorList按照依赖关系进行排序, 这里会把一个class内部的因子排在一起, dependency正确排序
            factorList = self.sort_factorsGivenDependency(factorList)

            # 先批量执行classFunc
            for factorName in factorList:
                # 获取这个因子的class, 判断有没有设置对应的classFunc
                class_ = self.factor_cfg[factorName]["class"]
                if class_ not in classList: # 被执行/判断过了
                    continue
                classList.remove(class_)    # 进入执行/判断分支
                if class_ not in self.class_cfg.keys():
                    continue
                funcList = self.class_cfg[class_]
                if funcList in [None, []]:
                    continue
                # 说明是有效的classFunc
                for funcName in funcList:
                    res = self.func_map[funcName](self)
                    if isinstance(res, dict):
                        self.dolphindb_cmd+=res["cmd"]
                    else:
                        self.dolphindb_cmd+=res

            # 再分别执行因子计算函数factorFunc
            for factorName in factorList:
                # 看一下有没有midFunc, 如果有的话需要获取midFunc
                midFuncList = self.factor_cfg[factorName]["dependency"]["midFunc"]
                if midFuncList:
                    for midFunc in midFuncList:
                        res = self.func_map[midFunc](self)
                        if isinstance(res, dict):
                            res = res["cmd"]
                        self.dolphindb_cmd+= res
                # 获取这个因子的计算函数
                calFuncName = self.factor_cfg[factorName]["calFunc"]
                paramsDict = self.factor_cfg[factorName]  # 获取这个factor的一且信息
                calFunc = self.func_map[calFuncName]
                nParams = calFunc.__code__.co_argcount
                if nParams == 3:
                    self.dolphindb_cmd+= calFunc(self,factorName,paramsDict)
                else:
                    self.dolphindb_cmd += calFunc(self,factorName)
        # 运行
        self.dolphindb_cmd+=self.update_data() # 上传至数据库的SQL语句
        # self.session.run(self.dolphindb_cmd)    # 运行

if __name__ == "__main__":
    import func.factorfunc0903 as calFunc
    import func.classfunc0903 as classFunc
    import func.midfunc0903 as midFunc
    with open(r".\config\factor.json5", "r",encoding='utf-8') as f:
        factor_cfg = json5.load(f)
    with open(r".\config\indicator.json5","r",encoding='utf-8') as f:
        indicator_cfg = json5.load(f)
    with open(r".\config\class.json5","r",encoding='utf-8') as f:
        class_cfg = json5.load(f)
    session=ddb.session()
    # session.connect("localhost",8848,"admin","123456")
    session.connect("172.16.0.184",8001,"maxim","dyJmoc-tiznem-1figgu")
    # pool=ddb.DBConnectionPool("172.16.0.184",8001,10,"maxim","dyJmoc-tiznem-1figgu")
    F = FactorCalculator(session=session, config=config,
                         factor_cfg=factor_cfg,
                         indicator_cfg=indicator_cfg,
                         func_map=get_funcMapFromImport(midFunc, classFunc, calFunc),
                         class_cfg=class_cfg)
    # F.init_database(True,True,True,True)
    # F.set_factorList(factor_list=list(factor_cfg.keys()))
    F.set_factorList(factor_list=["shio_avg20_plus"])
    F.run(start_date="20200101",end_date="20250430")
    print(F.dolphindb_cmd)
