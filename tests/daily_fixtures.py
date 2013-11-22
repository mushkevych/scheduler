__author__ = 'Bohdan Mushkevych'


def generate_hourly_vertical_composite_key(index, total):
    h = 0
    d = 20010303
    site = 0
    for i in range(total):
        h += 1
        if h % 3:
            site += 1
        if h > 23:
            h = 0
            d += 1
            if d > 20010331:
                d = 20010303

        if i == index:
            break

    return 'domain_name_%s' % str(site), str(d) + '_%02d' % h


def generate_hourly_horizontal_composite_key(index, total):
    h1 = '20010303_10'
    h2 = '20010303_11'

    if index <= total / 2:
        return 'domain_name_%s' % str(index), h1
    else:
        return 'domain_name_%s' % str(index), h2


if __name__ == '__main__':
    pass