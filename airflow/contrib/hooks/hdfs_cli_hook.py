import logging
import subprocess
from csv import DictReader
from datetime import datetime
from tempfile import NamedTemporaryFile

from airflow import configuration
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.utils.file import TemporaryDirectory
from airflow.utils.helpers import popen_and_tail


class HDFSCliHook(BaseHook):
    '''
    HDFSCliHook is a wrap around "hadoop fs" cli tool
    '''
    def __init__(self, hdfs_cli_conn_id='hdfs_cli_default', run_as=None):
        try:
            self.hdfs_cli_conn_id = hdfs_cli_conn_id
            self.conn = self.get_connection(hdfs_cli_conn_id)
            self.binary = self.conn.extra_dejson.get('binary', 'hadoop fs').split(' ')
        except:
            self.binary = ['hadoop', 'fs']

    def run_cli(self, arguments, verbose=True):
        hdfs_cmd = self.binary + arguments
        logging.info('running hdfs command "{0}"'.format(' '.join(hdfs_cmd)))
        (self.sp, tailer) = popen_and_tail(hdfs_cmd)
        (return_code, stdout, stderr) = tailer()
        if return_code:
            raise AirflowException(stdout, stderr)
        if verbose:
            logging.info(stderr)
        return stdout

    def cat(self, path, ignoreCrc=False):
        arguments = ['-cat']
        if ignoreCrc:
            arguments.append('-ignoreCrc')
        arguments.append(path)
        return self.run_cli(arguments)

    def checksum(self, path):
        arguments = ['-checksum', path]
        return self.run_cli(arguments)

    def chgrp(self, group, path, recursive=False):
        arguments = ['-chgrp']
        if recursive:
            arguments.append('-R')
        arguments += [group, path]
        return self.run_cli(arguments)

    def chmod(self, mode, path, recursive=False):
        arguments = ['-chmod']
        if recursive:
            arguments.append('-R')
        arguments += [mode, path]
        return self.run_cli(arguments)

    def chown(self, owner, path, recursive=False):
        arguments = ['-chown']
        if recursive:
            arguments.append('-R')
        arguments += [owner, path]
        return self.run_cli(arguments)

    def text(self, path):
        arguments = ['-text', path]
        return self.run_cli(arguments)

    def copyFromLocal(self, localSrc, dst, force=False):
        arguments = ['-copyFromLocal']
        if force:
            arguments.append('-f')
        arguments += [localSrc, dst]
        return self.run_cli(arguments)

    def writeText(self, text, dst, force=False):
        with TemporaryDirectory(prefix='airflow_hdfsop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                f.write(text.encode('UTF-8'))
                f.flush()
                fname = f.name
                return self.copyFromLocal(fname, dst, force)

    def appendToFile(self, localSrc, dst):
        arguments = ['-appendToFile', localSrc, dst]
        return self.run_cli(arguments)

    def appendText(self, text, dst):
        with TemporaryDirectory(prefix='airflow_hdfsop_') as tmp_dir:
            with NamedTemporaryFile(dir=tmp_dir) as f:
                f.write(text.encode('UTF-8'))
                f.flush()
                fname = f.name
                return self.appendToFile(fname, dst)

    def ls(self, path, directory=False, recursive=False):
        '''
        list files for pattern <path>
        username or filename containing space might cause line parsing failed
        '''
        def parse_file_info(file_info_raw):
            print 'parsing {0}'.format(repr(file_info_raw))
            file_info_raw['replica'] = \
                -1 if file_info_raw['replica'] == '-' \
                   else int(file_info_raw['replica'])
            file_info_raw['size'] = int(file_info_raw['size'])
            file_info_raw['date'] = \
                datetime.strptime(file_info_raw['date'], '%Y-%m-%d').date()
            file_info_raw['time'] = \
                datetime.strptime(file_info_raw['time'], '%H:%M').time()
            return file_info_raw

        arguments = ['-ls']
        if directory:
            arguments.append('-d')
        if recursive:
            arguments.append('-R')
        arguments.append(path)

        try:
            file_raw_lines = self.run_cli(arguments).splitlines()[1:]
        except AirflowException as e:
            if e[1].endswith('ls: `{0}\': No such file or directory\n'.format(path)):
                file_raw_lines = []
            else:
                raise e
        files_raw = DictReader([' '.join(l.split()) for l in file_raw_lines],
                               delimiter=' ',
                               skipinitialspace=True,
                               fieldnames=['permissions', 'replica', 'owner',
                               'group', 'size', 'date', 'time', 'name'])

        files_info = map(parse_file_info, files_raw)
        return files_info

    def rm(self, path, recursive=False, force=False, skipTrash=False):
        arguments = ['-rm']
        if recursive:
            arguments.append('-r')
        if force:
            arguments.append('-f')
        if skipTrash:
            arguments.append('-skipTrash')
        arguments.append(path)
        return self.run_cli(arguments)

    def kill(self):
        if hasattr(self, 'sp'):
            if self.sp.poll() is None:
                print("Killing the Hadoop job")
                self.sp.kill()
