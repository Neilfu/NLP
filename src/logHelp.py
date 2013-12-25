import sys     
import logging

LEVELS={'DEBUG':logging.DEBUG,
        'INFO':logging.INFO,
        'WARNING':logging.WARNING,
        'ERROR':logging.ERROR,
        'CRITICAL':logging.CRITICAL,
        }

def setLog(strLevel='INFO',logfile='../log.txt'):
    level=LEVELS[strLevel]
    logging.basicConfig(
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S',
                )
    logger = logging.getLogger()
    handler = logging.FileHandler(logfile)
    logger.addHandler(handler)
    #console = logging.StreamHandler()
    #logger.addHandler(console)
    logger.setLevel(level)
    return logger

def progressBar(num=1, total=100, bar_word=".", sep= 1 ):
    stars=('-','\\','|','/')
    rate = int(float(num) / float(total)*1000)
    if rate % sep == 0:
        sys.stdout.write(str(rate/10.0)+'%  '+stars[(rate / sep) % len(stars)]+"\r")
        sys.stdout.flush() 
        