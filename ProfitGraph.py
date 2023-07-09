# coding: utf-8
#!/usr/bin/python3

import requests
import yaml

import ccxt

#import libs.ccxt    #<---------BTCMEX対応のため neo_duelbotのlibs/ccxt 以下と libs/utils をカレントフォルダに置くと使えます

import calendar
import copy
from datetime import datetime,timedelta
import hashlib
import hmac
import json
import re
from requests.auth import AuthBase
from requests import Request, Session
from requests.exceptions import HTTPError
import time
import traceback
import urllib.parse

from logging import getLogger, ERROR, WARNING, INFO, DEBUG, StreamHandler, Formatter
def setup_logger():
    logger = getLogger(__name__)
    logger.setLevel(DEBUG)
    handler = StreamHandler()
    handler.setFormatter(Formatter(fmt='%(asctime)s.%(msecs)03d:  %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
    handler.setLevel(INFO)
    logger.addHandler(handler)
    return logger

class database:
    def __init__(self, logger, host='', port=8086, database=''):

        self._logger = logger
        self.__last_value = {}
        
        # Influx DBとの接続（もしインストールされていれば）
        try:
            from influxdb import InfluxDBClient
            import urllib3
            from urllib3.exceptions import InsecureRequestWarning
            urllib3.disable_warnings(InsecureRequestWarning)
        except Exception as e:
            self._logger.exception("Influxdb module import error : {}, {}".format(e, traceback.print_exc()))

        if host!='' and database!='' :
            try:
                self.__client = InfluxDBClient(host=host, port=port, database=database)
                self.__client.query('show measurements')  # 接続テスト
            except Exception as e:
                self._logger.error("Influxdb connection error : {}".format(e))
                self.__client = None
        else:
            self._logger.info("Skip connecting for Influxdb")
            self.__client = None


    def write( self, measurement, tags='', **kwargs ):
        try:
            fields = copy.deepcopy( kwargs )
            if self.__client != None :
                if tags!='' and 'exchange' in tags:

                    last_value_dict = self.__last_value.get(tags['exchange'],{})
                    for key,val in kwargs.items():

                        # 前回の値(self.__last_valueに保存)から呼び出す
                        last_value = last_value_dict.get(key,0)

                        if last_value==0 :
                            # 前回の値が無い場合には influxdb へ問い合わせて最後の値を呼び出す
                            query = "select last(\"{}\") from \"balance\" where \"exchange\"='{}'".format(key,tags['exchange'])
                            result = list(self.__client.query(query).get_points(measurement='balance'))
                            if result :
                                last_value = result[-1]['last']

                        if last_value!=0 :
                           # 変化分をキーにして格納
                            fields['diff_'+key]=float(val-last_value)
                        else:
                            fields['diff_'+key]=float(0)

                        last_value_dict[key]=val

                    self.__last_value[tags['exchange']] = last_value_dict

            if tags=='':
                data = [{"measurement": measurement, "fields": kwargs}]
            else:
                data = [{"measurement": measurement, "tags": tags, "fields": fields}]

            if self.__client != None :
                self.__client.write_points(data)

            else:
                self._logger.info( data )
        except Exception as e:
            self._logger.exception("Influxdb write error : {}, {}".format(e, traceback.print_exc()))
            return ""

        return kwargs


class online_information():
    def _update(self, target):
        # 直近30秒アップデートされていなければ取得する
        while target['update_time']+30<time.time() :
            try:
                target['update_handler']()
            except Exception as e:
                self._logger.exception("Error while getting {} : {}, {}".format(target['name'], e, traceback.print_exc()))
                if target['price'] != 0:
                    break
                time.sleep(10)
        
    
class exchange_rate(online_information):

    def __init__(self, logger, db):
        self._logger = logger
        self._db = db
        self._bitflyer_publicapi = ccxt.bitflyer()

        self.__usdjpy = {'name': 'USDJPY', 'price':0, 'update_time':time.time()-100, 'update_handler': self.__update_usdjpy}

        self.__xbtusd = {'name': 'XBTUSD', 'price':0, 'update_time':time.time()-100, 'update_handler': self.__update_xbtusd}
        self.__ethusd = {'name': 'ETHUSD', 'price':0, 'update_time':time.time()-100, 'update_handler': self.__update_ethusd}

        self.__btcjpy = {'name': 'BTC_JPY', 'price':0, 'update_time':time.time()-100, 'update_handler': self.__update_btcjpy}
        self.__fxbtcjpy = {'name': 'FX_BTC_JPY', 'price':0, 'update_time':time.time()-100, 'update_handler': self.__update_fxbtcjpy}

    def __update_usdjpy(self):
        # Get JPYUSD from Gaitame-online
        res = requests.get('https://www.gaitameonline.com/rateaj/getrate').json()
        for q in res['quotes']:
            if (q['currencyPairCode'] == 'USDJPY'):
                self.__usdjpy['price'] = (float(q['bid']) + float(q['ask'])) / 2.0
                self.__usdjpy['update_time'] = time.time()
                break
        self._logger.info( "update USDJPY={:.1f}".format(self.__usdjpy['price']) )
        if self.__xbtusd['price']!=0:
            self._db.write( measurement="mex_market",
                        xbtusd=float(self.__xbtusd['price']),
                        usdjpy=float(self.__usdjpy['price']),
                        xbtjpy=float(self.__usdjpy['price']*self.__xbtusd['price']))

    def __update_xbtusd(self):
        unixtime = calendar.timegm(datetime.utcnow().utctimetuple())
        url = 'https://www.bitmex.com/api/udf/history?symbol=XBTUSD&resolution=60&from=' + str(int(unixtime)-60) + '&to=' + str(unixtime)
        ohlcv = requests.get(url).json()
        self.__xbtusd['price'] = (ohlcv['h'][0] + ohlcv['l'][0]) / 2.0
        self.__xbtusd['update_time'] = time.time()
        self._logger.info( "update XBTUSD={:.1f}".format(self.__xbtusd['price']) )
        if self.__usdjpy['price']!=0:
            self._db.write( measurement="mex_market",
                        xbtusd=float(self.__xbtusd['price']),
                        usdjpy=float(self.__usdjpy['price']),
                        xbtjpy=float(self.__usdjpy['price']*self.__xbtusd['price']))

    def __update_ethusd(self):
        ticker = requests.get('https://api.bybit.com/v2/public/tickers').json()
        self.__ethusd['price'] = float([s['last_price'] for s in ticker['result'] if s['symbol']=='ETHUSD'][0])
        self.__ethusd['update_time'] = time.time()
        self._logger.info( "update ETHUSD={:.1f}".format(self.__ethusd['price']) )

    def __update_btcjpy(self):
        self.__btcjpy['price'] = int( self._bitflyer_publicapi.public_get_getticker(params={"product_code":"BTC_JPY"})['ltp']) 
        self.__btcjpy['update_time'] = time.time()
        self._logger.info( "update BTCJPY={:.0f}".format(self.__btcjpy['price']) )
        if self.__fxbtcjpy['price']!=0:
            self._db.write( measurement="bf_market",
                        fx=float(self.__fxbtcjpy['price']),
                        spot=float(self.__btcjpy['price']))

    def __update_fxbtcjpy(self):
        self.__fxbtcjpy['price'] = int( self._bitflyer_publicapi.public_get_getticker(params={"product_code":"FX_BTC_JPY"})['ltp']) 
        self.__fxbtcjpy['update_time'] = time.time()
        self._logger.info( "update FXBTCJPY={:.0f}".format(self.__fxbtcjpy['price']) )
        if self.__btcjpy['price']!=0:
            self._db.write( measurement="bf_market",
                        fx=float(self.__fxbtcjpy['price']),
                        spot=float(self.__btcjpy['price']))

    def __get_price(self, target):
        self._update(target)
        return target['price']

    @property
    def usdjpy(self):
        return self.__get_price(self.__usdjpy)

    @property
    def xbtusd(self):
        return self.__get_price(self.__xbtusd)

    @property
    def ethusd(self):
        return self.__get_price(self.__ethusd)

    @property
    def xbtjpy(self):
        return self.usdjpy * self.xbtusd / 100000000

    @property
    def fxbtcjpy(self):
        return self.__get_price(self.__fxbtcjpy)

    @property
    def btcjpy(self):
        return self.__get_price(self.__btcjpy)


class bitmex_info(online_information):
    def __init__(self, logger, db):
        self._logger = logger
        self._db = db
        self.__instrument = {'name':'Open Interest/OpenValue', 'open_interest':0, 'open_value':0, 'update_time':time.time()-100, 'update_handler': self.__update_instrument}

    def __update_instrument(self):
        instrument = requests.get("https://www.bitmex.com/api/v1/instrument?symbol=XBTUSD&reverse=true").json()[0]
        self.__instrument['open_interest'] = instrument['openInterest']
        self.__instrument['open_value'] = instrument['openValue']
        self.__instrument['update_time'] = time.time()
        self.__instrument['price'] = instrument['openInterest']
        self._logger.info( "MEX: Open_Interest={:,.0f} / Open_Value={:,.0f}".format(self.__instrument['open_interest'],self.__instrument['open_value']) )

        self._db.write( measurement="mex_market",
                        open_interest=int(self.__instrument['open_interest']),
                        open_value=int(self.__instrument['open_value']))

    @property
    def open_interest(self):
        self._update(self.__instrument)
        return self.__instrument['open_interest']

    @property
    def open_value(self):
        self._update(self.__instrument)
        return self.__instrument['open_value']

class gmo_api(AuthBase):
    def __init__(self, api_key, secret):
        self.api_key, self.secret = api_key, secret
        self.s = Session()
        self.s.headers['Content-Type'] = 'application/json'
        self.s.auth = self

    def __call__(self, r):
        timestamp = str(int(time.time() * 1000))
        o = urllib.parse.urlparse(r.path_url).path
        p = re.sub(r'\A/(public|private)', '', o)
        text = timestamp + r.method + p + (r.body or '')
        sign = hmac.new(self.secret.encode('ascii'), text.encode('ascii'), hashlib.sha256).hexdigest()
        headers = dict(r.headers)
        headers['API-KEY'] = self.api_key
        headers['API-TIMESTAMP'] = timestamp
        headers['API-SIGN'] = sign
        r.prepare_headers(headers)
        return r

    def _get(self, path, payload):
        req = Request('GET', 'https://api.coin.z.com/private' + path, params=payload)
        prepped = self.s.prepare_request(req)
        try:
            resp = self.s.send(prepped)
            resp.raise_for_status()
        except HTTPError as e:
            resp = None
            print(e)
        return resp

class exchange():
    def __init__(self, logger, name, items, rate, db):
        self._logger = logger
        self._exchange_type = items['type']
        self._name = name
        self._rate = rate
        self._db = db

        self._unreal = 0
        self._balance = 0

        if self._exchange_type=='BF' :
            self._api = ccxt.bitflyer({'apiKey':items['apiKey'], 'secret':items['secret']})

        elif self._exchange_type=='Liquid' :
            self._api = ccxt.liquid({'apiKey':items['apiKey'], 'secret':items['secret']})

        elif self._exchange_type=='BITMEX' :
            self._api = ccxt.bitmex({'apiKey':items['apiKey'], 'secret':items['secret']})

        elif self._exchange_type=='BYBIT' :
            self._api = ccxt.bybit({'apiKey':items['apiKey'], 'secret':items['secret']})

#        elif self._exchange_type=='BTCMEX' :
#            self._api = libs.ccxt.btcmex({'apiKey':items['apiKey'], 'secret':items['secret'], 'timeout':10000, 'options':{'api-expires':5}})
        elif self._exchange_type=='PHEMEX' :
            self._api = ccxt.phemex({'apiKey':items['apiKey'], 'secret':items['secret']})

        elif self._exchange_type=='GMO' :
            self._api = gmo_api(items['apiKey'], items['secret'])

        else:
            self._api = None

    def write_balance_to_db(self):
        if self._exchange_type == 'BF':
            return self.__get_balance_bitflyer()
        elif self._exchange_type == 'Liquid':
            return self.__get_balance_liquid()
        elif self._exchange_type == 'BITMEX':
            return self.__get_balance_bitmex()
        elif self._exchange_type == 'BYBIT':
            return self.__get_balance_bybit()
#        elif self._exchange_type == 'BTCMEX':
#            return self.__get_balance_btcmex()
        elif self._exchange_type == 'PHEMEX':
            return self.__get_balance_phemex()
        elif self._exchange_type == 'GMO':
            return self.__get_balance_gmo()
        else:
            return 0, 0, "Unsupported exchange : {}".format(self._exchange_type)
        
    def __get_balance_bitflyer(self):
        try:
            # 現物残高
            spot_balance = float(0)
            spot_btc = float(0)
            res = self._api.fetch_balance()['info']
            for r in res :
                try:
                    if r['currency_code']=='JPY' :   spot_balance += float(r['amount'])
                    elif r['currency_code']=='BTC' :
                        spot_balance += float(r['amount'])*self._rate.btcjpy
                        spot_btc += float(r['amount'])
                except:
                    pass

            # 証拠金残高
            collteral = 0
            res = self._api.private_get_getcollateralaccounts()
            for c in res:
                try:
                    if c['currency_code']=='JPY' : collteral += float(c['amount'])
                    if c['currency_code']=='BTC' :
                        collteral += float(c['amount'])*self._rate.btcjpy
                        spot_btc += float(c['amount'])
                except:
                    pass

            self._balance = collteral+spot_balance

            # オープンポジション
            res = self._api.private_get_getcollateral()
            self._unreal = res['open_position_pnl']

            positions = self._api.private_get_getpositions( params={"product_code":"FX_BTC_JPY"} )
            if not positions:
                total_position_btc = 0
                average_price = 0
            else:
                size = []
                price = []
                for pos in positions:
                    if "product_code" in pos:
                        size.append(pos["size"])
                        price.append(pos["price"])
                        side = pos["side"]

                average_price = round(sum(price[i] * size[i] for i in range(len(price))) / sum(size))
                sum_size = round(sum(size), 9)
                total_position_btc = round(float(sum_size if side == 'BUY' else -sum_size), 8)

        except Exception as e:
            if res:
                self._logger.info( "Rresponce from getting balance : {}".format(res) )
            self._logger.error("Error while bitflyer getting balance[{}] : {}".format(self._name,e))
            return 0,0,""

        db_str = self._db.write( measurement="balance", tags={'exchange' : self._name,},
                        fixjpy=float(self._balance),
                        jpy=float(self._balance+self._unreal),
                        fixbtc=self._balance/self._rate.btcjpy,
                        btc=(self._balance+self._unreal)/self._rate.btcjpy,
                        pos=float(total_position_btc),
                        price=float(average_price),
                        spot=float(spot_btc),)

        return self._balance, self._unreal, db_str

    def __get_balance_liquid(self):
        try:
            # 現物残高
            spot_balance = float(0)
            spot_btc = float(0)
            res = self._api.fetch_balance()
            if 'fiat_accounts' in res['info'] :
                balance_list = res['info']['fiat_accounts']
            else:
                balance_list = res['info']
            for r in balance_list:
                try:
                    if r['currency']=='JPY' :   spot_balance += float(r['balance'])
                    elif r['currency']=='BTC' :
                        spot_balance += float(r['balance'])*self._rate.btcjpy
                        spot_btc += float(r['balance'])
                except:
                    pass

            self._balance = spot_balance

            # ポジション
#            res = self._api.fetch_orders()
            # 未対応
            self._unreal = 0
            average_price = 0
            total_position_btc = 0

        except Exception as e:
            if res:
                self._logger.info( "Rresponce from getting balance : {}".format(res) )
            self._logger.error("Error while liquid getting balance[{}] : {}".format(self._name,e))
            return 0,0,""

        db_str = self._db.write( measurement="balance", tags={'exchange' : self._name,},
                        fixjpy=float(self._balance),
                        jpy=float(self._balance+self._unreal),
                        fixbtc=self._balance/self._rate.btcjpy,
                        btc=(self._balance+self._unreal)/self._rate.btcjpy,
                        pos=float(total_position_btc),
                        price=float(average_price),
                        spot=float(spot_btc),)

        return self._balance, self._unreal, db_str

    def __get_balance_bitmex(self):
        res = None
        try:
            res = excnahge.fetch_balance()
            self._balance = int(res['info'][0]['walletBalance'])
            self._unreal = int(res['info'][0]['unrealisedPnl'])
        except Exception as e:
            if res:
                self._logger.info( "Rresponce from fetch_balance : {}".format(res) )
            self._logger.error("Error while bitmex fetch_balance[{}] : {}".format(self._name,e))
            return 0,0,""

        pos = None
        try:
            pos = excnahge.private_get_position()
            posi = 0
            quarter_posxbt = 0
            for i in range(len(pos)) :
                if pos[i]['symbol']=='XBTUSD' :
                    posi = int(pos[i]['currentQty'])
                    avg = int(pos[i]['avgEntryPrice']) if pos[i]['avgEntryPrice']!= None else self._rate.xbtusd
                else :
                    currentQty = float(int(pos[i]['currentQty'])/self._rate.xbtusd)
                    self._logger.info( "    {} : {:.3f}".format(pos[i]['symbol'],currentQty) )
                    quarter_posxbt += currentQty

        except Exception as e:
            if pos:
                self._logger.info( "Rresponce from private_get_position_list : {}".format(pos) )
            self._logger.error("Error while bitmex private_get_position_list[{}] : {}".format(self._name,e))
            return 0,0,""

        db_str = self._db.write( measurement="balance", tags={'exchange' : self._name,},
                        fixjpy=float(self._balance*self._rate.xbtjpy),
                        jpy=float(self._balance+self._unreal)*self._rate.xbtjpy,
                        fixbtc=self._balance/100000000,
                        btc=(self._balance+self._unreal)/100000000,
                        pos=float(posi/self._rate.xbtusd+quarter_posxbt+res['info'][0]['walletBalance']/100000000),
                        price=float(avg),
                        spot=self._balance/100000000,)

        return self._balance*self._rate.xbtjpy, self._unreal*self._rate.xbtjpy, db_str

    def __get_balance_bybit(self):
        res = None
        try:
            res = self._api.fetch_balance()
            res_usdt = self._api.fetch_balance({"coin":"USDT"})
            res_eth = self._api.fetch_balance({"coin":"ETH"})

            self._balance = int(res['info']['result']['BTC']['wallet_balance']*100000000)+ \
                            int(res_usdt['info']['result']['USDT']['wallet_balance']*self._rate.usdjpy/self._rate.xbtjpy)+ \
                            int(res_eth['info']['result']['ETH']['wallet_balance']*self._rate.ethusd/self._rate.xbtusd*100000000)
            self._unreal = int(res['info']['result']['BTC']['unrealised_pnl']*100000000)+ \
                           int(res_usdt['info']['result']['USDT']['unrealised_pnl']*self._rate.usdjpy/self._rate.xbtjpy)+ \
                            int(res_eth['info']['result']['ETH']['unrealised_pnl']*self._rate.ethusd/self._rate.xbtusd*100000000)
        except Exception as e:
            if res:
                self._logger.info( "Rresponce from fetch_balance : {}".format(res) )
            if res_usdt:
                self._logger.info( "Rresponce from fetch_balance : {}".format(res_usdt) )
            self._logger.error("Error while bybit fetch_balance[{}] : {}".format(self._name,e))
            return 0,0,""

        pos = None
        try:

            pos = self._api.v2_private_get_position_list({'symbol' : 'BTCUSD'})['result']
            posi = pos['size'] if pos['side']=='Buy' else -pos['size']
            avg = pos['entry_price']
        except Exception as e:
            if pos:
                self._logger.info( "Rresponce from private_get_position_list : {}".format(pos) )
            self._logger.error("Error while bybit private_get_position_list[{}] : {}".format(self._name,e))
            return 0,0,""

        db_str = self._db.write( measurement="balance", tags={'exchange' : self._name,},
                        fixjpy=float(self._balance*self._rate.xbtjpy),
                        jpy=float(self._balance+self._unreal)*self._rate.xbtjpy,
                        fixbtc=self._balance/100000000,
                        btc=(self._balance+self._unreal)/100000000,
                        pos=float(posi/self._rate.xbtusd),
                        price=float(avg),
                        spot=self._balance/100000000,)
        
        return self._balance*self._rate.xbtjpy, self._unreal*self._rate.xbtjpy, db_str

    def __get_balance_btcmex(self):
        res = None
        try:
            res = self._api.fetch_balance()['info'][0]
            self._balance = res['walletBalance']
            self._unreal = res['unrealisedPnl']
        except Exception as e:
            if res:
                self._logger.info( "Rresponce from fetch_balance : {}".format(res) )
            self._logger.error("Error while btcmex fetch_balance[{}] : {}".format(self._name,e))
            return 0,0,""

        pos = None
        try:
            pos = self._api.private_get_position()[0]
            if pos==[] :
                posi = 0
                avg = 0
            else:
                posi = pos['currentQty']
                avg = pos['avgEntryPrice']

        except Exception as e:
            if pos:
                self._logger.info( "Rresponce from private_get_position_list : {}".format(pos) )
            self._logger.error("Error while btcmex private_get_position_list[{}] : {}".format(self._name,e))
            return 0,0,""

        db_str = self._db.write( measurement="balance", tags={'exchange' : self._name,},
                        fixjpy=float(self._balance*self._rate.xbtjpy),
                        jpy=float(self._balance+self._unreal)*self._rate.xbtjpy,
                        fixbtc=self._balance/100000000,
                        btc=(self._balance+self._unreal)/100000000,
                        pos=float(posi/self._rate.xbtusd),
                        price=float(avg),
                        spot=self._balance/100000000,)


        return self._balance*self._rate.xbtjpy, self._unreal*self._rate.xbtjpy, db_str


    def __get_balance_phemex(self):
        res = None
        try:
            # 現物残高
            res = self._api.fetch_balance()
            spot_dict = dict([(d['currency'],float(d.get('balanceEv',0))) for d in res.get('info',{}).get('data',[])])

            spot_balance = spot_dict['BTC']*self._rate.btcjpy
            spot_btc = spot_dict['BTC']
        except Exception as e:
            if res:
                self._logger.info( "Rresponce from fetch_balance : {}".format(res) )
            self._logger.error("Error while btcmex fetch_balance[{}] : {}".format(self._name,e))
            self._logger.info(traceback.format_exc())
            return 0,0,""

        try:
            pos = self._api.privateGetAccountsPositions({'currency':'BTC'})

            self._balance = pos['data']['account']['accountBalanceEv'] + spot_btc
            self._unreal = sum([p['unRealisedPnlEv'] for p in pos['data']['positions']])

            posi=0
            for p in pos['data']['positions']:
                if p['currency']!="BTC" :
                    continue
                if p['side']=='Buy':
                    posi += p['size']
                elif p['side']=='Sell':
                    posi = -p['size']
                avg = p['avgEntryPriceEp']/10000

        except Exception as e:
            if pos:
                self._logger.info( "Rresponce from private_get_position_list : {}".format(pos) )
            self._logger.error("Error while btcmex private_get_position_list[{}] : {}".format(self._name,e))
            self._logger.info(traceback.format_exc())
            return 0,0,""

        db_str = self._db.write( measurement="balance", tags={'exchange' : self._name,},
                        fixjpy=float(self._balance*self._rate.xbtjpy),
                        jpy=float(self._balance+self._unreal)*self._rate.xbtjpy,
                        fixbtc=self._balance/100000000,
                        btc=(self._balance+self._unreal)/100000000,
                        pos=float(posi/self._rate.xbtusd),
                        price=float(avg),
                        spot=spot_btc/100000000,)


        return self._balance*self._rate.xbtjpy, self._unreal*self._rate.xbtjpy, db_str


    def __get_balance_gmo(self):
        try:
            # 現物残高
            spot_balance = float(0)
            spot_btc = float(0)
            res = self._api._get('/v1/account/assets', {}).json()
            balance_list = res.get('data')
            for r in balance_list:
                try:
                    if r['symbol']=='JPY' :
                        spot_balance += float(r['amount'])
                    elif r['symbol']=='BTC' :
                        spot_balance += float(r['amount'])*self._rate.btcjpy
                        spot_btc += float(r['amount'])
                except:
                    pass

            self._balance = spot_balance

            # ポジション
            positions = self._api._get('/v1/openPositions', {'symbol': 'BTC_JPY', 'page': None, 'count': None}).json().get('data',{}).get('list',{})

            if not positions:
                total_position_btc = 0
                average_price = 0
            else:
                size = []
                price = []
                for pos in positions:
                    if pos['side']=='BUY':
                        size.append(float(pos["size"]))
                        price.append(float(pos["price"]))
                    elif pos['side']=='SELL':
                        size.append(-float(pos["size"]))
                        price.append(-float(pos["price"]))

                average_price = round(sum(price[i] * size[i] for i in range(len(price))) / sum(size))
                total_position_btc = round(sum(size), 9)

            # 未対応
            self._unreal = sum([int(p['lossGain']) for p in positions])

        except Exception as e:
            if res:
                self._logger.info( "Rresponce from getting balance : {}".format(res) )
            self._logger.error("Error while liquid getting balance[{}] : {}".format(self._name,e))
            return 0,0,""

        db_str = self._db.write( measurement="balance", tags={'exchange' : self._name,},
                        fixjpy=float(self._balance),
                        jpy=float(self._balance+self._unreal),
                        fixbtc=self._balance/self._rate.btcjpy,
                        btc=(self._balance+self._unreal)/self._rate.btcjpy,
                        pos=float(total_position_btc),
                        price=float(average_price),
                        spot=float(spot_btc),)

        return self._balance, self._unreal, db_str


if __name__ == "__main__":

    parameters = yaml.safe_load(open('ProfitGraph.yaml', 'r', encoding='utf-8_sig') )
    exchange_list = parameters['markets']

    logger = setup_logger()

    db = database(logger=logger, host='localhost', port=8086, database='bots')
#    db = database(logger=logger)

    rate = exchange_rate(logger, db)
    bitmex = bitmex_info(logger, db)

    minutes_counter = -1
    _today = '00'
    while True:
        time.sleep(1)
        # ポジション履歴を 1 分毎に記録して保存
        current_minutes = int(time.time() / 60)
        if minutes_counter != current_minutes:
            minutes_counter = current_minutes

            total_balance = 0
            total_unreal = 0
            currenttime = (datetime.utcnow()+timedelta(hours=9)).timestamp()
            message = ''
            message_mex = ''
            mex_usd_total = mex_positon_total = mex_unreal_total = 0

            for name,items in exchange_list.items():
                try:
                    if 'exchange' not in items:
                        items['exchange'] = exchange(logger, name, items, rate, db)

                    balance,unreal,db_str = items['exchange'].write_balance_to_db()
                    if db_str :
                        print( "Load collateral : {:>15} balance:{:>11.0f} unreal:{:>+6.0f} {}".format(name,balance,unreal,db_str) )

                except Exception as e:
                    traceback.print_exc()
            print( "Bitmex OI/OV = {:,.0f}/{:,.0f}".format(bitmex.open_interest, bitmex.open_value) )
            print( "bitFlyer FX/Spot = {:,.0f}/{:,.0f} : {:.1f}%".format(rate.fxbtcjpy, rate.btcjpy, (rate.fxbtcjpy-rate.btcjpy)/rate.btcjpy*100) )
