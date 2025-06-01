import time


def go2sleep():
    idx = 0
    while True:
        idx += 1
        time.sleep(1)
        print(f'sleeping {idx}')
