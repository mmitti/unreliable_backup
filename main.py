# -*- coding: utf-8 -*-
import sqlite3
import argparse
import os
import shutil
import json
import hashlib
import stat
class buckup_data:
    def __init__(self, cursol, dir = None):
        self.dir = dir
        if self.dir is None:self.dir = ""
        self.file_data = {}
        self.child_dir = []
        self.is_new = True
        self.cursol = cursol
    def save(self):
        if(self.is_new):
            self.cursol.execute("""INSERT INTO backup_data(dir, file_data, child_dir) VALUES(?, ?, ?);""", (self.dir, json.dumps(self.file_data), json.dumps(self.child_dir)))
            self.is_new = False
        else:
            self.cursol.execute("""UPDATE backup_data set file_data=?, child_dir=?  WHERE dir=?;""", (json.dumps(self.file_data), json.dumps(self.child_dir), self.dir))
    def delete(self):
        if(not self.is_new):
            self.cursol.execute("""DELETE from backup_data WHERE dir=?;""", (self.dir,))

    def read(self, dir = None):
        if dir is None:dir = self.dir
        self.cursol.execute("""SELECT * from backup_data;""")
        data = self.cursol.fetchone()
        if(data is not None):
            self.dir = data[0]
            self.file_data = json.loads(data[1])
            self.child_dir = json.loads(data[2])
            self.is_new = False
    def get_file_hash(self, name):
        return self.file_data.get(name, 0)

    def has_file(self, name):
        return name in self.file_data

    def add_file(self, name, fhash):
        self.file_data[name] = fhash
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
        return md5.hexdigest()
    except OSError as e:
         print("HASH I/O error({0}): {1}".format(e.errno, e.strerror))
    return -1

def file_exists(path):
    try:
        return os.path.exists(path)
    except OSError as e:
         print("FILE_EXISTS I/O error({0}): {1}".format(e.errno, e.strerror))
    return False

def file_copy(s, d):
    try:
        shutil.copy2(s,d)
        file_stat = os.stat(s)
        os.chown(d, file_stat[stat.ST_UID], file_stat[stat.ST_GID])
    except IOError as e:
         print("FILE_CP I/O error({0}): {1}".format(e.errno, e.strerror))
def file_remove(path):
    try:
        os.remove(path)
    except OSError as e:
         print("FILE_RM I/O error({0}): {1}".format(e.errno, e.strerror))
def dir_remove(path):
    try:
        shutil.rmtree(path)
    except IOError as e:
         print("RM_TREE I/O error({0}): {1}".format(e.errno, e.strerror))
def make_dirs(path):
    try:
        os.makedirs(path)
    except IOError as e:
         print("MK_DIR I/O error({0}): {1}".format(e.errno, e.strerror))
# srcの中身を から dest へコピーする
def exec_copy(src_dir, dest_dir, cursol):
    print("ENTER {0} ".format(src_dir))
    data = buckup_data(cursol, src_dir)
    data.read()
    #srcにデータが存在しない場合
    #dbにデータが存在するなどの場合に起きうる
    if(not file_exists(src_dir)):
        print("DIR DELETED")
        data.delete()
        if file_exists(dest_dir):
            dir_remove(dest_dir)
        for f in data.child_dir:
            exec_copy(src_dir+'/'+f, dest_dir+'/'+f, cursol)
        return
    if(not file_exists(dest_dir)):
        print("DEST MKDIR")
        make_dirs(dest_dir)
        shutil.copystat(src_dir, dest_dir)
        file_stat = os.stat(src_dir)
        os.chown(dest_dir, file_stat[stat.ST_UID], file_stat[stat.ST_GID])
    exists_files = []
    exists_dirs = []

    #ファイルコピー
    print(os.listdir(src_dir))
    for l in os.listdir(src_dir):

        new_src = src_dir+'/'+l
        new_dest = dest_dir+'/'+l
        if os.path.islink(new_src):
            file_copy(new_src, new_dest)
            data.add_file(l, 0)
            exists_files.append(l)
        elif os.path.isdir(new_src):
            exec_copy(new_src, new_dest, cursol)
            data.add_child(l)
            exists_dirs.append(l)
        else:
            fhash = get_file_hash(new_src)
            #ハッシュが違うorハッシュがない
            if not fhash == data.get_file_hash(l):
                print("CP {0} #{1}".format(new_src, fhash))
                file_copy(new_src, new_dest)
                data.add_file(l, fhash)
            exists_files.append(l)

    print("RM FILES")
    #ソースに無く、destに残っているゴミも消すため再帰的に処理
    delete_list = data.update_removed_files(exists_files)
    print(delete_list)
    for f in delete_list:
        file_remove(dest_dir+'/'+f)

    print("RM DIRS")
    delete_list = data.update_removed_dirs(exists_dirs)
    print(delete_list)
    for f in delete_list:
        exec_copy(src_dir+'/'+f, dest_dir+'/'+f, cursol)
    data.save()
    print("EXIT {0}".format(src_dir))
def backup(arg):
    parser = argparse.ArgumentParser(description='バックアップモード', usage='%(prog)s backup [-h] target [target ...] dest db [options..]')
    parser.add_argument('target', nargs='+', help='バックアップソースのディレクトリ')
    parser.add_argument('dest', type=str, help='データ保存先')
    parser.add_argument('db', type=str, help='検証用DB保存先。データ保存先と別の場所にしてください。')
    args = parser.parse_args(arg)
    conn = sqlite3.connect(args.db)
    cur = conn.cursor()
    try:
        cur.execute("""CREATE TABLE backup_data(dir text, file_data text, child_dir text);""")
    except:
        print("A")
    for t in args.target:
        exec_copy(t, args.dest+'/'+os.path.basename(t), cur)
    conn.commit()
    cur.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='信用できないストレージにバックアップするツール。\n気が向いたらエラー訂正も入れる。')
    parser.add_argument('mode', choices=['backup'], help='モード')
    parser.add_argument('args', nargs=argparse.REMAINDER, help='引数')
    args = parser.parse_args()
    if(args.mode == 'backup'):
        backup(args.args)
