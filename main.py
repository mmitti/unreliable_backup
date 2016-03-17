# -*- coding: utf-8 -*-
import sqlite3
import argparse
import os
import shutil
import json
import hashlib
import stat
import base64
import traceback

import logging
import threading
import sys

logger =  logging.getLogger(__name__)
class backup_stat:
    def __init__(self):
        self.files = 0
        self.dirs = 0
        self.cpfiles= 0
        self.cplinks = 0
        self.mkdirs = 0
        self.rmfiles = 0
        self.rmdirs = 0
        self.skip = 0
        self.current = ""

class buckup_data:
    def __init__(self, conn, dir = None):
        self.dir = dir
        if self.dir is None:self.dir = ""
        self.file_data = {}
        self.child_dir = []
        self.is_new = True
        self.connection = conn
    def save(self):
        cur = self.connection.cursor()
        if(self.is_new):
            cur = self.connection.cursor()
            cur.execute("""INSERT INTO backup_data(dir, file_data, child_dir) VALUES(?, ?, ?);""", (self.dir, json.dumps(self.file_data), json.dumps(self.child_dir)))
            self.is_new = False
        else:
            cur.execute("""UPDATE backup_data set file_data=?, child_dir=?  WHERE dir=?;""", (json.dumps(self.file_data), json.dumps(self.child_dir), self.dir))
        self.connection.commit()
        cur.close()
    def delete(self):
        if(not self.is_new):
            cur = self.connection.cursor()
            cur.execute("""DELETE from backup_data WHERE dir=?;""", (self.dir,))
            self.connection.commit()
            cur.close()

    def read(self, dir = None):
        if dir is None:dir = self.dir
        cur = self.connection.cursor()
        cur.execute("""SELECT * from backup_data WHERE dir=?;""", (dir,))
        data = cur.fetchone()
        cur.close()
        if(data is not None):
            self.dir = data[0]
            self.file_data = json.loads(data[1])
            self.child_dir = json.loads(data[2])
            self.is_new = False
    def get_file_hash(self, name):
        r = self.file_data.get(name, None)
        if r is None: return 0
        return int.from_bytes(base64.b85decode(r), byteorder='big')


    def has_file(self, name):
        return name in self.file_data

    def add_file(self, name, fhash):
        self.file_data[name] = base64.b85encode(fhash.to_bytes(16, byteorder='big')).decode('utf-8')
    def add_child(self, name):
        if name in self.child_dir: return
        self.child_dir.append(name)

    def update_removed_files(self, exisits_files):
        removed_files = set(self.file_data.keys()) - set(exisits_files)
        for f in removed_files:
            self.file_data.pop(f)
        return list(removed_files)

    def update_removed_dirs(self, exisits_dirs):
        removed_dirs = set(self.child_dir) - set(exisits_dirs)
        for f in removed_dirs:
            self.child_dir.remove(f)
        return list(removed_dirs)


def get_file_hash(file_path):
    try:
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(2048 * md5.block_size), b''):
                md5.update(chunk)
        return int(md5.hexdigest(), 16)
    except OSError as e:
         logger.debug("HASH I/O error({0}): {1}".format(e.errno, e.strerror))
    return -1

def file_exists(path):
    try:
        return os.path.exists(path)
    except OSError as e:
         logger.debug("FILE_EXISTS I/O error({0}): {1}".format(e.errno, e.strerror))
    return False

def file_copy(s, d):
    try:
        shutil.copy2(s,d)
        file_stat = os.stat(s)
        os.chown(d, file_stat[stat.ST_UID], file_stat[stat.ST_GID])
    except IOError as e:
         logger.debug("FILE_CP I/O error({0}): {1}".format(e.errno, e.strerror))
         raise e
def link_copy(s, d):
    try:
        if file_exists(d):
            file_remove(d)
        os.symlink(os.readlink(s), d)
    except IOError as e:
         logger.debug("LINK_CP I/O error({0}): {1}".format(e.errno, e.strerror))
         raise e

def file_remove(path):
    try:
        os.remove(path)
    except OSError as e:
         logger.debug("FILE_RM I/O error({0}): {1}".format(e.errno, e.strerror))
         raise e
def dir_remove(path):
    try:
        shutil.rmtree(path)
    except IOError as e:
         logger.debug("RM_TREE I/O error({0}): {1}".format(e.errno, e.strerror))
         raise e
def make_dirs(path):
    try:
        os.makedirs(path)
    except IOError as e:
         logger.debug("MK_DIR I/O error({0}): {1}".format(e.errno, e.strerror))
         raise e
# srcの中身を から dest へコピーする
def exec_copy(src_dir, dest_dir, conn, stat):
    logger.debug("ENTER {0} ".format(src_dir))
    data = buckup_data(conn, src_dir)
    data.read()
    #srcにデータが存在しない場合
    #dbにデータが存在するなどの場合に起きうる
    if(not file_exists(src_dir)):
        logger.debug("DIR DELETED")
        data.delete()
        if file_exists(dest_dir):
            dir_remove(dest_dir)
            stat.rmdirs += 1
        for f in data.child_dir:
            exec_copy(src_dir+'/'+f, dest_dir+'/'+f, conn, stat)
        return
    if(not file_exists(dest_dir)):
        logger.debug("DEST MKDIR")
        make_dirs(dest_dir)
        shutil.copystat(src_dir, dest_dir)
        file_stat = os.stat(src_dir)
        os.chown(dest_dir, file_stat[stat.ST_UID], file_stat[stat.ST_GID])
        stat.mkdirs += 1
    exists_files = []
    exists_dirs = []

    #ファイルコピー
    logger.debug(os.listdir(src_dir))
    for l in os.listdir(src_dir):
        new_src = src_dir+'/'+l
        new_dest = dest_dir+'/'+l
        stat.current = l
        try:
            if os.path.islink(new_src):
                link_copy(new_src, new_dest)
                data.add_file(l, 0)
                exists_files.append(l)
                stat.cplinks += 1
            elif os.path.isdir(new_src):
                exec_copy(new_src, new_dest, conn, stat)
                data.add_child(l)
                exists_dirs.append(l)
                stat.dirs += 1
            else:
                fhash = get_file_hash(new_src)
                #ハッシュが違うorハッシュがない
                if not fhash == data.get_file_hash(l):
                    logger.debug("CP {0} #{1}".format(new_src, fhash))
                    file_copy(new_src, new_dest)
                    data.add_file(l, fhash)
                    stat.cpfiles += 1
                stat.files += 1
                exists_files.append(l)
        except:
            logger.error('SKIP {0}'.format(new_src))
            logger.debug(traceback.format_exc())
            stat.skip += 1

    logger.debug("RM FILES")
    #ソースに無く、destに残っているゴミも消すため再帰的に処理
    delete_list = data.update_removed_files(exists_files)
    logger.debug(delete_list)
    for f in delete_list:
        file_remove(dest_dir+'/'+f)
        stat.rmfiles += 1

    logger.debug("RM DIRS")
    delete_list = data.update_removed_dirs(exists_dirs)
    logger.debug(delete_list)
    for f in delete_list:
        exec_copy(src_dir+'/'+f, dest_dir+'/'+f, conn, stat)

    data.save()
    logger.debug("EXIT {0}".format(src_dir))

def backup_progress(stat, e):
    if e.is_set():
        sys.stderr.write('\r\033[K' + "FILE:{0} COPY_FILE:{1} RM_FILE:{2} DIR:{3} SKIP:{4}\t{5}".format(stat.files, stat.cpfiles, stat.rmfiles, stat.rmdirs, stat.skip, stat.current ))
        sys.stderr.flush()
        t=threading.Timer(1, backup_progress, (stat, e))
        t.start()

def backup(arg):
    parser = argparse.ArgumentParser(description='バックアップモード', usage='%(prog)s backup [-h] target [target ...] dest db [options..]')
    parser.add_argument('target', nargs='+', help='バックアップソースのディレクトリ')
    parser.add_argument('dest', type=str, help='データ保存先')
    parser.add_argument('db', type=str, help='検証用DB保存先。データ保存先と別の場所にしてください。')
    args = parser.parse_args(arg)
    conn = sqlite3.connect(args.db)

    handler = logging.StreamHandler()
    handler.setLevel(logging.WARN)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)8s %(message)s"))
    logger.addHandler(handler)

    try:
        cur = conn.cursor()
        cur.execute("""CREATE TABLE backup_data(dir text, file_data text, child_dir text);""")
        conn.commit()
        cur.close()
        logger.debug("MK TABLE")
    except:
        logger.debug("LOAD TABLE")
    stat = backup_stat()
    e = threading.Event()
    e.set()
    t=threading.Timer(1, backup_progress, (stat, e))
    t.start()
    for t in args.target:
        t = t.rstrip('/\\')
        exec_copy(t, args.dest+'/'+os.path.basename(t), conn, stat)
    e.clear()
    print("DONE\nFILE:{0} DIR:{1}\nCOPY FILE:{2} LINK:{3} MKDIR:{4}\nRM FILE:{5} DIR:{6} SKIP:{7}".format(
        stat.files, stat.dirs , stat.cpfiles, stat.cplinks, stat.mkdirs, stat.rmfiles, stat.rmdirs, stat.skip))





if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='信用できないストレージにバックアップするツール。\n気が向いたらエラー訂正も入れる。')
    parser.add_argument('mode', choices=['backup'], help='モード')
    parser.add_argument('args', nargs=argparse.REMAINDER, help='引数')
    args = parser.parse_args()
    logger.setLevel(logging.DEBUG)
    if(args.mode == 'backup'):
        backup(args.args)
