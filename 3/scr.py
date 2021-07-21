import os
import time
from datetime import datetime

class DataFile:
    def __init__(self, path):
        self.path = path
        self.name = self.path.split('/')[-1]
        self.modification_time = self.get_modification_time()
        self.data = self.get_data()
        self.changing_difference = int((now-self.modification_time).total_seconds()/60)

    def get_data(self):
        try:
            with open(self.path, 'r') as f:
                data = f.read().splitlines()
            return data
        except Exception as ex:
            print(f"Can't open file <{self.name}> as text file")


    def get_modification_time(self):
        time_mod_str = time.ctime(os.path.getmtime(self.path))
        # print(time_mod_str)
        time_mod_date_time = datetime.strptime(time_mod_str, "%a %b %d %H:%M:%S %Y")
        return time_mod_date_time

class ConfigFile(DataFile):
    def __init__(self, path):
        super().__init__(path)
        self.path = path
        self.data = self.get_data()

    def get_data(self):
        with open(self.path, 'r') as f:
            data = f.read().splitlines()
        config = {param.split(': ')[0]:param.split(': ')[1] for param in data}
        return config

def check_default_conf ():
    for file in files:
        if file.name in config_default.data:
            # print(f"{file.name}, {file.modification_time}, {file.changing_difference}")
            if file.changing_difference > timeout:
                print(f"The file <{file.name}> wasn't changed for {timeout} minutes")
        else:
            print(f"The entry <{file.name}> wasn't found in conf file {config_default.name}")

def check_other_conf ():
    for file in files:
        ch_file_in_configs = False
        for conf in configs:
            # print (True) if conf.data['time'] == '0' else None
            if file.name in conf.data['name']:
                ch_file_in_configs = True
                # print(f"File {file.name} exists in config {conf.name}")
                for sing in file.data:
                    ch_sing_in_conf = False
                    # print (f"{sing} >>> {conf.data['sign']}")
                    if sing == conf.data['sign']:
                        ch_sing_in_conf = True
                        conf_time = int(conf.data['time']) if conf.data['time'] !='0' else timeout
                        # print(f'Signature {sing} exist in conf file {conf.name}, timeout {conf_time}')
                        if file.changing_difference > conf_time:
                            pass
                            # print (f"Signature <{sing}> has been in conf <{conf.name}> for conf time <{conf_time}>")
                        else:
                            print (f"{file.name}file.changing_difference {file.changing_difference}")
                            print(f"Signature <{sing}> hasn't been in conf <{conf.name}> for conf time <{conf_time}>")
                    if not ch_sing_in_conf: print (f"Signature <{sing}> doesn't exits in config {conf.name}")
        if not ch_file_in_configs: print(f"File <{file.name}> doesn't exist in configs")
    # for file in files:
    #     for conf in configs:
    #         # print (True) if conf.data['time'] == '0' else None
    #         if file.name in conf.data['name']:
    #             # print(f"File {file.name} exists in config {conf.name}")
    #             for sing in file.data:
    #                 # print (f"{sing} >>> {conf.data['sign']}")
    #                 if sing == conf.data['sign']:
    #                     conf_time = int(conf.data['time']) if conf.data['time'] !='0' else timeout
    #                     print(f'Signature {sing} exist in conf file {conf.name}, timeout {conf_time}')


if __name__ == '__main__':
    ch_iter = 0
    while True:
        timeout = 10
        ch_iter = ch_iter + 1

        now = datetime.strptime(str(datetime.now()).split('.')[0], "%Y-%m-%d %H:%M:%S")
        files = [DataFile(os.path.join(os.getcwd(), 'val1/'+file)) for file in os.listdir(path=os.path.join(os.getcwd(), 'val1'))]
        configs = [ConfigFile(os.path.join(os.getcwd(), 'val2/'+file)) for file in os.listdir(path=os.path.join(os.getcwd(), 'val2')) if not file.startswith('default')]
        config_default = DataFile(os.path.join(os.getcwd(), 'val2/default.cfg'))

        # file = DataFile(os.path.join(os.getcwd(), 'val1/file_5.txt'))
        # print(now)

        print(f"Check Iter <{ch_iter}>, {now}")
        ch_iter = ch_iter+1
        check_default_conf()
        print('#'*100)
        check_other_conf()
        time.sleep(15)
        # time.sleep(900) Every 15 minutes