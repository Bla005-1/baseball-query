import math

def percentile(a, q):
    a = sorted(a)
    k = (len(a)-1) * q/100.0
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return a[int(k)]
    d0 = a[int(f)] * (c - k)
    d1 = a[int(c)] * (k - f)
    return d0 + d1

pi = math.pi

def arctan2(y, x):
    return math.atan2(y, x)

def degrees(rad):
    return rad * 180.0 / math.pi


def isnan(x):
    return x != x

