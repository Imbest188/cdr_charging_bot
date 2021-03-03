from ftpretty import ftpretty as ftplib

class Ftp:
    def __init__(self, host, login, password):
        self.ftp = ftplib(host, login, password)
        self.host = host
        self.login = login
        self.password = password
        self.currentPath = []

    def cd(self, item):
        self.ftp.cd(item)
        if len(item):
            self.currentPath.append(item)
        return self.list()

    def list(self, path=''):
        targetPath = ''
        if len(path):
            targetPath = path
        else:
            self.getCurrentPath()
        try:
            return self.ftp.list(path)
        except:
            self.ftp = ftplib(self.host, self.login, self.password)
            return self.ftp.list(path)
        #return self.ftp.list(self.getCurrentPath(), extra=True)

    def getCurrentPath(self):
        path = '/'
        if len(self.currentPath):
            path += '/'.join(self.currentPath)
            path += '/'
        return path

    def back(self):
        if len(self.currentPath):
            self.currentPath.pop()
            self.ftp.cd(self.getCurrentPath())
        return self.list()

    def pathInfo(self):
        return self.list()

    def download(self, remotePath, file, localPath='CDR/'):
        with open(localPath + file, 'wb+') as downloadingFile:
            if remotePath.endswith('/'):
                pass
            else:
                remotePath += '/'
            try:
                self.ftp.get(remotePath + file, downloadingFile)
            except:
                self.ftp = ftplib(self.host, self.login, self.password)
                self.ftp.get(remotePath + file, downloadingFile)
                
