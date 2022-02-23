#! /usr/bin/python3
from subprocess import PIPE, Popen
import fcntl
import os
import select
import time
import re


class MainTest(object):
    def __init__(self, args):
        self.process = Popen(args, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        flags = fcntl.fcntl(self.process.stdout, fcntl.F_GETFL)
        fcntl.fcntl(self.process.stdout, fcntl.F_SETFL, flags | os.O_NONBLOCK)

    def send(self, data, tail='\n'):
        self.process.stdin.write((data+tail).encode())
        self.process.stdin.flush()

    def recv(self, t=0.1):
        pr = self.process.stdout
        while 1:
            if not select.select([pr], [], [], 0)[0]:
                time.sleep(t)
                continue
            content = pr.read()
            return content.decode()


class LimitTest(object):
    def __init__(self, arg):
        self.sampleCapacity = "insert {id} username{id} username{id}@test.com"
        self.sampleField = f"insert 1 {'a'*32} {'a'*255}"
        self.exit = ".exit"
        self.select = "select"
        self.tester = MainTest(arg)

    def function_test(self, i):
        output = ''
        cmds = [self.sampleCapacity.format(id=i), self.select]
        for c in cmds:
            self.tester.send(c)
            output += self.tester.recv()
        print(output)

    def test_table_capacity(self):
        max_capacity = 1400
        output = ''
        for i in range(max_capacity):
            sample = self.sampleCapacity.format(id=i)
            self.tester.send(sample)
            output += self.tester.recv()
            if re.search('Table full', output):
                print("Table full before reach theoretical capacity")
                self.tester.send(self.exit)
                return
        sample = self.sampleCapacity.format(id=max_capacity+1)
        self.tester.send(sample)
        output = self.tester.recv()
        if re.search('Table full', output):
            print("Table full as expected. Table capacity test succeeded.")
        else:
            print("Exceed table limitation. Table capacity test failed.")
        self.tester.send(self.exit)

    def test_field_capacity(self):
        output = ''
        self.tester.send(self.sampleField)
        output += self.tester.recv()
        self.tester.send(self.select)
        output += self.tester.recv()
        if re.search("a"*255, output) and re.search("a"*32, output):
            print("Field capacity test succeeded.")
        else:
            print("Field capacity test failed.")


if __name__ == '__main__':
    testArgs = ('./main',)
    tester = LimitTest(testArgs)
    for i in range(20):
        tester.function_test(i)
    tester.tester.send(tester.exit)
