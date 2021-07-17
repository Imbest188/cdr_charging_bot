from asndecoder import asndecoder as dec
import os
from ftp import Ftp
from threading import Thread
from multiprocessing.dummy import Pool as ThreadPool


class Processor:
    def __init__(self, ftp):
        self.work = False
        self.counter = 0
        self.errors = 0
        self.result = []
        self.__args = []
        self.filesSize = 0
        self.ftp = ftp
        self.func = None
        self.aborted = False

    def accuracyParse(self, file, arg):
        res = []
        with open('CDR/'+file+'.csv', 'r') as f:
            text = f.read()
            for line in text.split('\n'):
                args = line.split(';')
                if len(args) > 4:
                    if args[1] == arg or args[4] == arg:
                        res.append(line)
        return res

    def simpleParse(self, file, arg):
        res = []
        with open('CDR/'+file+'.csv', 'r') as f:
            text = f.read()
            for line in text.split('\n'):
                if arg in line:
                    res.append(line)
        return res

    def setFunc(self, func):
        self.func = func

    def parse(self, file):
        dec().decode(file)
        print(self.__args)
        for __arg in self.__args:
            if len(__arg) < 7:
                print('acc')
                self.result += self.accuracyParse(file, __arg)
            else:
                print('sim')
                self.result += self.simpleParse(file, __arg)

        try:
            print('remove ' + file)
            os.remove('CDR\\' + file)
            os.remove('CDR\\' + file + ".csv")
        except:
            print('ext rem')
            pass

    def prepare(self, file):
        if self.aborted:
            return
        try:
            pathParts = file.split('/',-1)
            filename = pathParts.pop()
            path = '/'.join(pathParts)
            print('download ' + filename)
            self.ftp.download(path, filename)
            print('parse ' + filename)
            self.parse(filename)
            print('complete ' + filename)
            self.counter += 1
        except:
            self.errors += 1

    def __process(self, files, args):
        self.work = True
        self.filesSize = len(files)
        self.__args = args
        self.counter = 0
        self.result = []
        self.errors = 0
        self.__args = args
        pool = ThreadPool(2)
        pool.map(self.prepare, files)
        pool.close()
        pool.join()
        print('OK - ', self.counter, ', ERR - ', self.errors)
        if self.func and not self.aborted:
            self.func(self.result)
        print(self.result)
        self.work = False

    def process(self, files, args):
        if self.work:
            return False
        Thread(target=self.__process,args=(files,args,),daemon=True).start()
        return True

    def state(self):
        if self.work:
            return str(self.counter)+'/'+str(self.filesSize) + ' completed, errors: ' + str(self.errors) + '\nFind ' + str(len(self.result)) + ' lines'
        return 'Idle'
    


class Worker:
    def __init__(self):
        self.ftp = Ftp('10.5.5.254','stat','mp26NokdpPpkM')
        self.process = Processor(self.ftp)
        self.sendFunc = None

    def setFunc(self, func):
        self.sendFunc = func
        self.process.setFunc(func)

    def processState(self):
        return self.process.state()

    def halfResult(self):
        self.sendFunc(self.process.result)

    def abort(self):
        self.process.aborted = True

    def parseFiles(self, timeRange, searchArguments):
        files = self.ftp.list('/msc/MSSLUG01/CHARGING/')
        files += self.ftp.list('/msc/MSSLUG02/CHARGING/')
        targetFiles = self.find(files, timeRange)
        self.process.process(targetFiles, searchArguments)
        return 'Найдено файлов: ' + str(len(targetFiles))

    def __find(self, files, timestamp1, timestamp2):
        result = []
        start = int(timestamp1)
        end = int(timestamp2)
        if start > end:
            start, end = end, start
        for file in files:
            pathParts = file.split('/',-1)
            filename = pathParts.pop()
            if len(filename) > 16:
                try:
                    timestamp = int(filename[7:17])
                    if timestamp >= start and timestamp <= end:
                        result.append(file)
                except:
                    pass
        return result

    def find(self, files, timestampts):
        result = []
        for time in timestampts:
            __result = self.__find(files, time[0], time[1])
            for item in __result:
                if item not in result:
                    result.append(item)
        return result

    
