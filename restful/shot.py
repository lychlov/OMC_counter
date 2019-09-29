from configures.omcs import omcs
from adapters.FTPAdapter import SFTPAdapter

ftp_a = SFTPAdapter('10.87.53.135', 'BZftpuser', 'Hw_20191001')
print(ftp_a.get_all_files('/opt/PRS/server/var/prs/result_file/20191001BZ'))
