import time
from clickhouse_driver import Client

client = Client(host='code.noxue.com',
                user="default",
                password="admin888",
                database='asy')


def lts2timestamp(l, t, s):
    l = l.strip()
    t = t.strip()
    s = s.strip()

    if len(t) < 6:
        t = '0' * (6 - len(t)) + t
    if len(s) < 3:
        s = '0' * (3 - len(s)) + s

    # 年月日时分秒毫秒 转 unix时间戳
    lt = l + t
    lts = time.strptime(
        lt[0:4] + "-" + lt[4:6] + "-" + lt[6:8] + " " + lt[8:10] + ":" +
        lt[10:12] + ":" + lt[12:14], "%Y-%m-%d %H:%M:%S")
    return str(int(time.mktime(lts))) + str(s)


def ticker(ticker_file):

    with open(ticker_file, 'r') as f:
        # 循环读取文件
        for line in f:
            # 跳过第一行
            if line.startswith('instId'):
                continue
            # csv分割字符串
            cols = line.split(',')

            # ts
            l = cols[2].strip()
            t = cols[3].strip()
            s = cols[4].strip()
            ts = lts2timestamp(l, t, s)

            direction = cols[5].strip()

            price = cols[6].strip()

            amount = cols[7].strip()

            # lts
            l = cols[8].strip()
            t = cols[9].strip()
            s = cols[10].strip()
            lts = lts2timestamp(l, t, s)
            print(ts + ',' + direction + ',' + price + ',' + amount + ',' +
                  lts)
            # li = [ord(c) for c in direction]
            # for _ in range(8 - len(li)):
            #     li.append(0)

            res = client.execute(
                '''insert into okx_btc_usdt_ticker(Lts, ts, price, amount, direction) values''',
                [{
                    'Lts': int(lts),
                    'ts': int(ts),
                    'price': float(price),
                    'amount': float(amount),
                    'direction': direction
                }])
            print(res)


def depth(depth_file):
    # 循环读取文件
    with open(depth_file, 'r') as f:
        # 循环读取文件
        for line in f:
            # 跳过第一行
            if line.startswith('instId'):
                continue
            # csv分割字符串
            cols = line.split(',')
            # ts
            l = cols[2].strip()
            t = cols[3].strip()
            s = cols[4].strip()
            ts = lts2timestamp(l, t, s)

            names = globals()
            index = 5
            for i in range(20):
                names["ask{}_price".format(i)] = cols[index].strip()
                index += 1
                names["ask{}_amount".format(i)] = cols[index].strip()
                index += 1

            for i in range(20):
                names["bid{}_price".format(i)] = cols[index].strip()
                index += 1
                names["bid{}_amount".format(i)] = cols[index].strip()
                index += 1

            if ask0_price == '0' or bid0_price == '0':
                continue

            # lts
            l = cols[index].strip()
            t = cols[index + 1].strip()
            s = cols[index + 2].strip()
            lts = lts2timestamp(l, t, s)

            # # 输出上面所有变量
            # for i in range(20):
            #     exec("print('ask{}_price:',ask{}_price)".format(i, i))
            #     exec("print('ask{}_amount:',ask{}_amount)".format(i, i))
            #     exec("print('bid{}_price:',bid{}_price)".format(i, i))
            #     exec("print('bid{}_amount:',bid{}_amount)".format(i, i))

            # 插入数据到 okx_btc_usdt_depth 表
            res = client.execute(
                '''insert into okx_btc_usdt_depth(
                        Lts,
                        ts,
                        ask0_price ,
                        ask1_price ,
                        ask2_price ,
                        ask3_price ,
                        ask4_price ,
                        ask5_price ,
                        ask6_price ,
                        ask7_price ,
                        ask8_price ,
                        ask9_price ,
                        ask10_price ,
                        ask11_price ,
                        ask12_price ,
                        ask13_price ,
                        ask14_price ,
                        ask15_price ,
                        ask16_price ,
                        ask17_price ,
                        ask18_price ,
                        ask19_price ,
                        ask0_amount ,
                        ask1_amount ,
                        ask2_amount ,
                        ask3_amount ,
                        ask4_amount ,
                        ask5_amount ,
                        ask6_amount ,
                        ask7_amount ,
                        ask8_amount ,
                        ask9_amount ,
                        ask10_amount ,
                        ask11_amount ,
                        ask12_amount ,
                        ask13_amount ,
                        ask14_amount ,
                        ask15_amount ,
                        ask16_amount ,
                        ask17_amount ,
                        ask18_amount ,
                        ask19_amount ,
                        bid0_price ,
                        bid1_price ,
                        bid2_price ,
                        bid3_price ,
                        bid4_price ,
                        bid5_price ,
                        bid6_price ,
                        bid7_price ,
                        bid8_price ,
                        bid9_price ,
                        bid10_price ,
                        bid11_price ,
                        bid12_price ,
                        bid13_price ,
                        bid14_price ,
                        bid15_price ,
                        bid16_price ,
                        bid17_price ,
                        bid18_price ,
                        bid19_price ,
                        bid0_amount ,
                        bid1_amount ,
                        bid2_amount ,
                        bid3_amount ,
                        bid4_amount ,
                        bid5_amount ,
                        bid6_amount ,
                        bid7_amount ,
                        bid8_amount ,
                        bid9_amount ,
                        bid10_amount ,
                        bid11_amount ,
                        bid12_amount ,
                        bid13_amount ,
                        bid14_amount ,
                        bid15_amount ,
                        bid16_amount ,
                        bid17_amount ,
                        bid18_amount ,
                        bid19_amount 
                    ) values(
                        %(Lts)s,
                        %(ts)s,
                        %(ask0_price)s,
                        %(ask1_price)s,
                        %(ask2_price)s,
                        %(ask3_price)s,
                        %(ask4_price)s,
                        %(ask5_price)s,
                        %(ask6_price)s,
                        %(ask7_price)s,
                        %(ask8_price)s,
                        %(ask9_price)s,
                        %(ask10_price)s,
                        %(ask11_price)s,
                        %(ask12_price)s,
                        %(ask13_price)s,
                        %(ask14_price)s,
                        %(ask15_price)s,
                        %(ask16_price)s,
                        %(ask17_price)s,
                        %(ask18_price)s,
                        %(ask19_price)s,
                        %(ask0_amount)s,
                        %(ask1_amount)s,
                        %(ask2_amount)s,
                        %(ask3_amount)s,
                        %(ask4_amount)s,
                        %(ask5_amount)s,
                        %(ask6_amount)s,
                        %(ask7_amount)s,
                        %(ask8_amount)s,
                        %(ask9_amount)s,
                        %(ask10_amount)s,
                        %(ask11_amount)s,
                        %(ask12_amount)s,
                        %(ask13_amount)s,
                        %(ask14_amount)s,
                        %(ask15_amount)s,
                        %(ask16_amount)s,
                        %(ask17_amount)s,
                        %(ask18_amount)s,
                        %(ask19_amount)s,
                        %(bid0_price)s,
                        %(bid1_price)s,
                        %(bid2_price)s,
                        %(bid3_price)s,
                        %(bid4_price)s,
                        %(bid5_price)s,
                        %(bid6_price)s,
                        %(bid7_price)s,
                        %(bid8_price)s,
                        %(bid9_price)s,
                        %(bid10_price)s,
                        %(bid11_price)s,
                        %(bid12_price)s,
                        %(bid13_price)s,
                        %(bid14_price)s,
                        %(bid15_price)s,
                        %(bid16_price)s,
                        %(bid17_price)s,
                        %(bid18_price)s,
                        %(bid19_price)s,
                        %(bid0_amount)s,
                        %(bid1_amount)s,
                        %(bid2_amount)s,
                        %(bid3_amount)s,
                        %(bid4_amount)s,
                        %(bid5_amount)s,
                        %(bid6_amount)s,
                        %(bid7_amount)s,
                        %(bid8_amount)s,
                        %(bid9_amount)s,
                        %(bid10_amount)s,
                        %(bid11_amount)s,
                        %(bid12_amount)s,
                        %(bid13_amount)s,
                        %(bid14_amount)s,
                        %(bid15_amount)s,
                        %(bid16_amount)s,
                        %(bid17_amount)s,
                        %(bid18_amount)s,
                        %(bid19_amount)s
                    )''', {
                    'Lts': int(lts),
                    'ts': int(ts),
                    "ask0_price": float(ask0_price),
                    "ask1_price": float(ask1_price),
                    "ask2_price": float(ask2_price),
                    "ask3_price": float(ask3_price),
                    "ask4_price": float(ask4_price),
                    "ask5_price": float(ask5_price),
                    "ask6_price": float(ask6_price),
                    "ask7_price": float(ask7_price),
                    "ask8_price": float(ask8_price),
                    "ask9_price": float(ask9_price),
                    "ask10_price": float(ask10_price),
                    "ask11_price": float(ask11_price),
                    "ask12_price": float(ask12_price),
                    "ask13_price": float(ask13_price),
                    "ask14_price": float(ask14_price),
                    "ask15_price": float(ask15_price),
                    "ask16_price": float(ask16_price),
                    "ask17_price": float(ask17_price),
                    "ask18_price": float(ask18_price),
                    "ask19_price": float(ask19_price),
                    "ask0_amount": float(ask0_amount),
                    "ask1_amount": float(ask1_amount),
                    "ask2_amount": float(ask2_amount),
                    "ask3_amount": float(ask3_amount),
                    "ask4_amount": float(ask4_amount),
                    "ask5_amount": float(ask5_amount),
                    "ask6_amount": float(ask6_amount),
                    "ask7_amount": float(ask7_amount),
                    "ask8_amount": float(ask8_amount),
                    "ask9_amount": float(ask9_amount),
                    "ask10_amount": float(ask10_amount),
                    "ask11_amount": float(ask11_amount),
                    "ask12_amount": float(ask12_amount),
                    "ask13_amount": float(ask13_amount),
                    "ask14_amount": float(ask14_amount),
                    "ask15_amount": float(ask15_amount),
                    "ask16_amount": float(ask16_amount),
                    "ask17_amount": float(ask17_amount),
                    "ask18_amount": float(ask18_amount),
                    "ask19_amount": float(ask19_amount),
                    "bid0_price": float(bid0_price),
                    "bid1_price": float(bid1_price),
                    "bid2_price": float(bid2_price),
                    "bid3_price": float(bid3_price),
                    "bid4_price": float(bid4_price),
                    "bid5_price": float(bid5_price),
                    "bid6_price": float(bid6_price),
                    "bid7_price": float(bid7_price),
                    "bid8_price": float(bid8_price),
                    "bid9_price": float(bid9_price),
                    "bid10_price": float(bid10_price),
                    "bid11_price": float(bid11_price),
                    "bid12_price": float(bid12_price),
                    "bid13_price": float(bid13_price),
                    "bid14_price": float(bid14_price),
                    "bid15_price": float(bid15_price),
                    "bid16_price": float(bid16_price),
                    "bid17_price": float(bid17_price),
                    "bid18_price": float(bid18_price),
                    "bid19_price": float(bid19_price),
                    "bid0_amount": float(bid0_amount),
                    "bid1_amount": float(bid1_amount),
                    "bid2_amount": float(bid2_amount),
                    "bid3_amount": float(bid3_amount),
                    "bid4_amount": float(bid4_amount),
                    "bid5_amount": float(bid5_amount),
                    "bid6_amount": float(bid6_amount),
                    "bid7_amount": float(bid7_amount),
                    "bid8_amount": float(bid8_amount),
                    "bid9_amount": float(bid9_amount),
                    "bid10_amount": float(bid10_amount),
                    "bid11_amount": float(bid11_amount),
                    "bid12_amount": float(bid12_amount),
                    "bid13_amount": float(bid13_amount),
                    "bid14_amount": float(bid14_amount),
                    "bid15_amount": float(bid15_amount),
                    "bid16_amount": float(bid16_amount),
                    "bid17_amount": float(bid17_amount),
                    "bid18_amount": float(bid18_amount),
                    "bid19_amount": float(bid19_amount)
                })
            print(res)


if __name__ == '__main__':
    ticker('files/ticker.csv')
    depth('files/depth.csv')
