import requests
import xml.etree.ElementTree as ET
import os
import pyarrow.fs as fs
from BUNSYO.logger import logger
from urllib.parse import quote, unquote
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def ls_volume(dtapfs, _path):
    try:
        file_info = dtapfs.get_file_info(fs.FileSelector(_path, recursive=False))
        return [os.path.basename(info.path) for info in file_info if info.type == fs.FileType.File]
    except Exception as e:
        msg = f'Failed to list files in path {_path}. Error: {str(e)}'
        logger.error(msg)
        raise Exception(msg)

def propfind(url, auth):
    headers = {
        'Depth': '1',
        'Content-Type': 'application/x-www-form-urlencoded'
    }
    data = "<D:propfind xmlns:D='DAV:' xmlns:Z='http://www.northgrid.co.jp/proself/'><D:prop><D:resourcetype /></D:prop></D:propfind>" 

    response = requests.request('PROPFIND',
                                url,
                                headers=headers,
                                data=data,
                                verify=False,
                                auth=auth)
    check_status(response.status_code)
    return response.text

def check_status(status_code):
    if status_code != 207:
        msg = f'WebDAV connection issue. Status code: {status_code}'
        logger.error(msg)
        raise Exception(msg)

def parse_response(response_text):
    namespaces = {'d': 'DAV:', 'z': 'http://www.northgrid.co.jp/proself/'}
    tree = ET.fromstring(response_text)
    items = []
    for response in tree.findall('d:response', namespaces):
        href = response.find('d:href', namespaces).text
        resourcetype = response.find('.//d:resourcetype', namespaces)
        is_dir = resourcetype is not None and resourcetype.find('d:collection', namespaces) is not None
        items.append((href, is_dir))
    return items

def get_filelist(method, base_url, src_path, auth):

    visited, files = set(), []
    if method == 'GET':
        logger.info('download method:GET')
        for url in base_url:
            file_name = '/' + url.split('/')[-1]
            files.append((url, file_name))
    elif method == 'PROPFIND':
        logger.info('download method:PROPFIND')
        for base in base_url:
            def _recursive_propfind(url):
                if url in visited:
                    return
                visited.add(url)
                response_text = propfind(url, auth)
                items = parse_response(response_text)
                for href, is_dir in items:
                    if href.rstrip('/') != url.rstrip('/'):
                        full_path = pathjoin(base, href)
                        if is_dir:
                            _recursive_propfind(full_path)
                        else:
                            href = href[len(quote(src_path)) + 1:] if href.startswith('/' + quote(src_path)) else href
                            files.append((full_path, unquote(href)))

            url = make_url(base, src_path)
            _recursive_propfind(url)
    return files

def make_url(_base_url, _file_path):
    return pathjoin(_base_url, quote(_file_path))

def pathjoin(*_path_list):
    _path_list = [
        _path[1:] if _path[0] == '/' else _path for _path in _path_list
    ]
    _path_list = [
        _path[:-1] if ((_path[-1] == '/') and (_path[-3:] != '://')) else _path
        for _path in _path_list
    ]
    return os.path.join(*_path_list)

def get_download_list(file_list, filter_target):

    # ファイル名のフィルタリング設定がある場合file_listをフィルタリングする
    if filter_target:
        filetarget_list = []
        for path, file in file_list:
            filename = os.path.basename(file).lower()
            if any(filename.startswith(target.lower()) for target in filter_target):
                filetarget_list.append((path, file))
    else:
        filetarget_list = file_list
        
    return filetarget_list

def preprocess(dtapfs, dest_path, method, base_url, src_path, auth, file_list_csv, filter_target):    
    exist_files = ls_volume(dtapfs, dest_path)

    # 収集対象ファイルリスト取得
    file_list = get_filelist(method, base_url, src_path, auth)
    download_list = get_download_list(file_list, filter_target)
    
    return download_list, exist_files