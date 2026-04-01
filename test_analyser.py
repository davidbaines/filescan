# test_analyser.py

from scanner import Scanner

from analyser import Analyser



if __name__ == "__main__":

    s = Scanner("config.yml")

    s.scan()

    a = Analyser("config.yml")

    a.analyse()

