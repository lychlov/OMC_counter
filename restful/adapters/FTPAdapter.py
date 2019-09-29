import ftplib
import paramiko


# import sftp


class FtpAdapter(object):

    def __init__(self, host, port, username, password):
        self.f = ftplib.FTP()
        self.f.connect(host=host, port=port)
        self.f.login(username, password)

    def close(self):
        self.f.close()

    def download(self, remote_file, local_file=None):
        if local_file is None:
            local_file = ''  # 默认存放目录
        fp = open(local_file, 'wb')
        self.f.retrbinary('RETR %s' % remote_file, fp.write, 1024)
        fp.close()

    def get_files(self, remote_dir, date, hour):
        self.f.cwd(remote_dir)
        return self.f.nlst()


class SFTPAdapter(object):
    def __init__(self, ip, username, password, timeout=30):
        self.ip = ip
        self.username = username
        self.password = password
        self.timeout = timeout
        # transport和chanel
        self.t = ''
        self.chan = ''
        # 链接失败的重试次数
        self.try_times = 3

    def connect(self):
        pass

    def close(self):
        pass

    def sftp_get(self, remotefile, localfile):
        t = paramiko.Transport(sock=(self.ip, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
        sftp.get(remotefile, localfile)
        t.close()

    def get_all_files(self, remote_dir):
        t = paramiko.Transport(sock=(self.ip, 22))
        t.connect(username=self.username, password=self.password)
        sftp = paramiko.SFTPClient.from_transport(t)
        all_files = list()

        # 去掉路径字符串最后的字符'/'，如果有的话
        if remote_dir[-1] == '/':
            remote_dir = remote_dir[0:-1]

        # 获取当前指定目录下的所有目录及文件，包含属性值
        files = sftp.listdir_attr(remote_dir)
        for x in files:
            # remote_dir目录中每一个文件或目录的完整路径
            filename = remote_dir + '/' + x.filename
            # 如果是目录，则递归处理该目录，这里用到了stat库中的S_ISDIR方法，与linux中的宏的名字完全一致
            all_files.append(filename)
        t.close()
        return all_files
