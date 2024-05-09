import os
import argparse
import sqlite3
import json
import plistlib
import uuid
from loguru import logger
from pathlib import Path
from shutil import copyfile
from datetime import datetime
from tempfile import mkstemp



cmd_args = {}
key_files = {}

def path2str(pth):
    return str(pth).replace(":\\", "-").replace("\\", "-").replace("/", "-").replace(" ", '_').replace(".", "_")

def convert_sqlite_to_dict(db_name, sqlite_file, strict=False):  
    db_content = {}
    connection = sqlite3.connect(sqlite_file)
    cur = connection.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cur.fetchall()
    tables_name = [i[0] for i in tables]
    for table_name in tables_name:
        try:
            cur.execute(f"pragma table_info({table_name})")
            columns_name = [i[1] for i in cur.fetchall()]
            cur.execute(f'SELECT * FROM {table_name}')
        except sqlite3.OperationalError as e:
            logger.warning(f'Select table {table_name} from {db_name} error with: {e}')
            if strict:
                exit(-1)
            else:
                continue
        
        rows = cur.fetchall()
        rows_n = []
        for row in rows:
            row_n = []
            for item in row:
                if isinstance(item, bytes):
                    row_n.append(item.decode('u8', errors='ignore'))
                else:
                    row_n.append(item)
            rows_n.append(row_n)    
        rows_n.insert(0, columns_name)
        db_content[table_name] = rows_n

    # 关闭数据库连接
    connection.close()
        
        
    # 将数据转为字典列表
    return db_content

def convert_plist_to_dict(plist_file):
    with open(plist_file, 'rb') as f:
        return  plistlib.load(f)

def is_sqlite(file_path):
    with open(file_path, 'rb') as f:
        magic_number = f.read(6)
    if magic_number == b"SQLite":
        return True
    return False

def is_plist(file_path):
    with open(file_path, 'rb') as f:
        magic_number = f.read(6)
    if magic_number == b'bplist':
        return True
    return False    

def ts2str(timestamp):
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d")
    except:
        return ""

def travel_dict(dic):
    for k, v in dic.items():
        if isinstance(v, bytes):
            if v[:6] == b"bplist":
                dic[k] = plistlib.loads(v)
            else:
                try:
                    dic[k] = v.decode("utf-8")
                except:
                    if not Path(path_n).exists():
                        os.mkdir(path_n)
                    tmp_file_name = path_n / str(uuid.uuid1())
                    with open(tmp_file_name, 'wb') as f:
                        f.write(v)                        
                    dic[k] = str(tmp_file_name.resolve()) 
        if isinstance(v, dict):
            travel_dict(v)

def selectKeyFiles(folder_path: Path, base_path: Path, verbose=False):    
    for file_path in folder_path.iterdir():
        # 检查文件是否是SQLite文件
        if file_path.is_file():
            if is_sqlite(file_path):
                stat = file_path.stat()
                key_files[str(file_path.relative_to(base_path))] = {
                    "info":{
                        "path": str(file_path.resolve()),
                        "type": "sqlite",
                        "size": stat.st_size,
                        "st_mtime": stat.st_mtime,
                        "mtime": ts2str(stat.st_mtime),
                        "st_atime": stat.st_atime,
                        "atime": ts2str(stat.st_atime),
                        "st_ctime": stat.st_ctime,
                        "ctime": ts2str(stat.st_ctime)
                    }
                }
                if verbose:
                    logger.success(f"{file_path} is sqlite file.\n{key_files[str(file_path.relative_to(base_path))]['info']}")
            elif is_plist(file_path):
                stat = file_path.stat()
                key_files[str(file_path.relative_to(base_path))] = {
                    "info":{
                        "path": str(file_path.resolve()),
                        "type": "plist",
                        "size": stat.st_size,
                        "st_mtime": stat.st_mtime,
                        "mtime": ts2str(stat.st_mtime),
                        "st_atime": stat.st_atime,
                        "atime": ts2str(stat.st_atime),
                        "st_ctime": stat.st_ctime,
                        "ctime": ts2str(stat.st_ctime)
                    }
                }
                if verbose:
                    logger.success(f"{file_path} is plist file.\n{key_files[str(file_path.relative_to(base_path))]['info']}")
        elif file_path.is_dir():
            selectKeyFiles(file_path, base_path, verbose)
        elif verbose:
            logger.info(f"{file_path} is not KEY file.")

def obtainKeyFiles(sorted_flag="mtime", verbose=False, strict=False):
    global key_files 
    if sorted_flag == "mtime":
        func = lambda key_file: key_files[key_file]["info"]["st_mtime"]
    elif sorted_flag == "ctime":
        func = lambda key_file: key_files[key_file]["info"]["st_ctime"]
    elif sorted_flag == "atime":
        func = lambda key_file: key_files[key_file]["info"]["st_atime"]
    elif sorted_flag == "size":
        func = lambda key_file: key_files[key_file]["info"]["size"]
    
    _, tmp_file = mkstemp() 
    key_files_name = sorted(key_files, key=func, reverse=True)
    for db_name in key_files_name:
        copyfile(key_files[db_name]["info"]["path"], tmp_file)
        try:
            if verbose:
                logger.info(f"Start to analyze {db_name}") 
            if key_files[db_name]["info"]["type"] == "sqlite":  
                db_json = convert_sqlite_to_dict(db_name, tmp_file)
            elif key_files[db_name]["info"]["type"] == "plist":
                db_json = convert_plist_to_dict(tmp_file)
            if verbose:
                logger.info(f"{db_name} analyze done.")  
        except Exception as e:
            db_json = {}
            logger.error(f'{db_name} analyze error.')
            logger.error(f'{key_files[db_name]}')
            logger.error(e)
            if strict:
                exit(-1)
        key_files[db_name]["content"] = db_json 
    travel_dict(key_files)   

def storeKeyFiles(format):              
    if format=='json':
        if len(key_files) > 20:
            path_n = opt / path2str(inp)
            if not path_n.exists():
                os.mkdir(path_n)
            for db_name, db_con in key_files.items():
                file_n = path_n / (path2str(db_name) + '.json')
                try:
                    with open(file_n, 'w', encoding='utf-8') as json_file:
                        json.dump(db_con, json_file, ensure_ascii=False, indent=2)     
                except Exception as e:
                    logger.error(f"Write json file error. File name: {file_n}")
                    with open(file_n, 'w', encoding='utf-8') as f:
                        f.write(db_con)         
        else:
            file_n = path2str(inp) + '.json'
            if Path(file_n).exists():
                choice = input("There is already a analysis result. Do you want to overwrite it? (y/n)")
            if choice == "y":
                with open(file_n, 'w', encoding='utf-8') as json_file:
                    json.dump(key_files, json_file, ensure_ascii=False, indent=2)
                
            
if __name__=="__main__":
    parser = argparse.ArgumentParser(description='将数据库转为json文件')
    parser.add_argument('-i', '--input', default=Path(".").resolve(), type=str, help='输入文件夹的路径')
    parser.add_argument('-o', '--output', default=Path(".").resolve(), type=str, help='输出文件的路径')  
    parser.add_argument('-s', '--sorted', default="mtime", type=str, help='排序方式: mtime, ctime, actime, size')      
    parser.add_argument('-f', '--format', default="json", type=str, help='输出格式: json, csv')      
    parser.add_argument('-v', '--verbose', action='store_true', help='启用详细输出')
    parser.add_argument('-st', '--strict', action='store_true', help='遇到错误退出')
    args = parser.parse_args()
    inp = Path(args.input)
    opt = Path(args.output).resolve()
    
    
    if not inp.exists():
        logger.error("Input path not exist.")
     
    # if opt.exists() and opt.is_dir():
    #     logger.warning(f"{opt} is directory, remove it.")
    #     os.remove(opt)
        
    selectKeyFiles(inp, base_path=inp, verbose=args.verbose)
    obtainKeyFiles(args.sorted, args.verbose, args.strict)
    
    storeKeyFiles(format=args.format)