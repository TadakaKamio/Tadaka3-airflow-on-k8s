import os
from BUNSYO.logger import logger

def open_hdfs_file(dtapfs, file_path):
    try:
        return dtapfs.open_input_stream(file_path, compression=None)
    except Exception as e:
        logger.error(f"Error: failed to get hdfs file {file_path}: {e}")
        raise e

def get_hdfs_file(dtapfs, dest_path, file_name):
    try:
        file_path = os.path.join(dest_path, file_name)
    
        with dtapfs.open_input_stream(file_path, compression=None) as hdfs_content:
            bytes_content = hdfs_content.read_buffer().to_pybytes()
            data = memoryview(bytes_content)
            return data
    except Exception as e:
        logger.error(f'Error: failed to get hdfs file {file_name}: {e}')
        raise e

def rename_file_extension(file_name, path):
    # ファイル名の変換
    basename, _ = os.path.splitext(file_name)
    new_file_name = f"{basename}.csv"
    # パスのファイル名部分の変換
    file_root, _ = os.path.splitext(path)
    new_file_path = f"{file_root}.csv"

    return new_file_name, new_file_path

def rename_uuid_filename(file_name, exist_files):
    import uuid

    extensions = []
    file = file_name
    while True:
        file, ext = os.path.splitext(file)
        if ext:
            extensions.insert(0, ext)
        else:
            break
    extension = ''.join(extensions)
    
    for _ in range(100):
        uuid_filename = str(uuid.uuid4()) + extension
        if uuid_filename not in exist_files:
            break
    else:
        raise Exception('Failed to generate unique HDFS path')
    
    return uuid_filename

def put_hdfs_file(dtapfs, dest_path, file_name, data):
    try:
        file_path = os.path.join(dest_path, file_name)
        
        # HDFSへファイルをアップロードする
        with dtapfs.open_output_stream(file_path) as hdfs_output:
            hdfs_output.write(data)
        logger.info(f'Success: reuploaded file {file_name} to HDFS.')
        return True
    except Exception as e:
        logger.error(f'Error: failed to put file {file_name} to HDFS: {e}')
        raise e

def delete_files(dtapfs, dest_path, file_name):
    try:
        dtapfs.delete_file(os.path.join(dest_path, file_name))
        logger.info(f"Deleted file: {file_name}")
    except Exception as e:
        logger.error(f"Failed to delete file: {file_name}, reason: {e}")
        raise e

def postprocess(dtapfs, dest_path, upload_file_list, csv_flag, uuid_flg, exist_files):
    # フラグチェック
    if csv_flag or uuid_flg:
        new_upload_file_list = []
        # HDFSのファイルをダウンロードし、加工その後HDFSへPUT
        for file_name, path, _ in upload_file_list:
            file_data = get_hdfs_file(dtapfs, dest_path, file_name)
            new_file_name = file_name
            new_file_path = path
            # csv形式変換
            if csv_flag:
                new_file_name, new_file_path = rename_file_extension(new_file_name, path)
            # UUIDへリネーム
            if uuid_flg:
                new_file_name = rename_uuid_filename(new_file_name, exist_files)
            # HDFSへPUT
            put_hdfs_file(dtapfs, dest_path, new_file_name, file_data)

            # HDFS上の元ファイルを削除
            delete_files(dtapfs, dest_path, file_name)

            new_upload_file_list.append((new_file_name, path, new_file_path))
    else:
        new_upload_file_list = upload_file_list
    
    return new_upload_file_list


