# 使用说明：https://github.com/GuyCheung/falcon-ngx_metric

import requests
import sys, urllib, time, json, traceback
from optparse import OptionParser
from configparser import ConfigParser

class Histogram(object):

    def __init__(self, values = None):
        self.values = []
        self.dirty = False

        self.add(values)

    def add(self, values):
        _t = type(values)

        if _t == int or _t == float:
            self.values.add(values)

        elif _t == list:
            self.values += values

        elif _t == str:
            v_s = map(lambda x: float(x), values.strip(', ').split(','))
            self.values += v_s

        elif _t == type(None):
            return

        self.dirty = True

    def calc(self):
        if not self.dirty or len(self.values) == 0:
            return

        self.values.sort()

#        self.sortValues = [self.values[0]]
#        for i in range(1, len(self.values)):
#            self.sortValues.insert(i, self.sortValues[i - 1] + self.values[i])

        self.dirty = False

    def percentile(self, percentile):
        if self.dirty:
            self.calc()

        if len(self.values) == 0:
            return 0.0

        pos = min(int(percentile * len(self.values)), len(self.values))
        #return float(self.sortValues[pos]) / (pos + 1)
        return self.values[pos]

    def percentiles(self):
        return [self.percentile(0.5), self.percentile(0.75), self.percentile(0.95), self.percentile(0.99)]


class Render(object):

    reserved_service_name = '__serv__'
    ts = int(time.time())

    @staticmethod
    def render(la):
        global renders
        c = renders[la[0]]

        if hasattr(c, '_before_render'):
            c._before_render(la)

        return c.render(la)

    @staticmethod
    def service_stat(c):
        global renders

        if hasattr(c, '_before_service_stat'):
            c._before_service_stat()

        return c.service_stat() if c.enable else []

    @staticmethod
    def get_service_name(ngx_host = None):
        global options

        if None != ngx_host and '' != ngx_host and options['use_ngx_host']:
            return ngx_host

        return options['service']

    @staticmethod
    def pack(name, tags, value):
        global options
        pack_func = 'pack_' + options['format']

        if not hasattr(Render, pack_func):
            sys.stderr.write("format type %s is not found!\n" % options['format'])
            return {}

        if 'service' not in tags.keys():
            tags['service'] = Render.get_service_name()

        f = getattr(Render, pack_func)
        return f(name, tags, value)

    @staticmethod
    def pack_odin(name, tags, value):
        return {
            'name': name,
            'value': value,
            'timestamp': Render.ts,
            'tags': tags,
        }

    @staticmethod
    def pack_falcon(name, tags, value):
        global options
        s = tags['service']
        del tags['service']

        return {
            'endpoint': s,
            'metric': name,
            'timestamp': int(time.time()),
            'step': options['falcon_step'],
            'value': value,
            'counterType': 'GAUGE',
            'tags': ','.join(map(lambda k: '%s=%s' % (k, tags[k]), tags.keys())),
        }

    @staticmethod
    def hash_default_get(h, key, default):
        return h[key] if key in h.keys() else default

    @staticmethod
    def hash_set_incr(h, key, inc):
        v = inc + Render.hash_default_get(h, key, 0)
        h[key] = v

class RenderQueryCount(Render):

    metric = 'query_count'

    service_count = {}
    enable = False

    @staticmethod
    def render(la):
        RenderQueryCount.enable = True

        if len(la) != 4:
            return []

        v = int(la[3])
        Render.hash_set_incr(RenderQueryCount.service_count, Render.get_service_name(la[1]), v)

        return Render.pack(RenderQueryCount.metric, {'api': la[2], 'service': Render.get_service_name(la[1])}, v)

    @staticmethod
    def _before_render(la):
        RenderErrRate.push(key=RenderQueryCount.metric, service=Render.get_service_name(la[1]), api=la[2], value=int(la[3]))

    @staticmethod
    def service_stat():
        res = []
        for service, value in RenderQueryCount.service_count.items():
            res.append(Render.pack(RenderQueryCount.metric,
                {'api': Render.reserved_service_name, 'service': service}, value))

        return res

    @staticmethod
    def _before_service_stat():
        for service, value in RenderQueryCount.service_count.items():
            RenderErrRate.push(key=RenderQueryCount.metric, service=service, api=Render.reserved_service_name, value=value)

class RenderUpstreamContacts(Render):
    metric = 'upstream_contacts'

    contacts = {}
    enable = False

    @staticmethod
    def render(la):
        RenderUpstreamContacts.enable = True

        if len(la) != 4:
            return []

        v = int(la[3])
        Render.hash_set_incr(RenderUpstreamContacts.contacts, Render.get_service_name(la[1]), v)

        return Render.pack(RenderUpstreamContacts.metric, {'api': la[2], 'service': Render.get_service_name(la[1])}, v)

    @staticmethod
    def service_stat():
        res = []
        for service, value in RenderUpstreamContacts.contacts.items():
            res.append(Render.pack(RenderUpstreamContacts.metric,
                {'api': Render.reserved_service_name, 'service': service}, value))

        return res


class RenderErrCount(Render):

    metric = 'error_count'

    service_count_detail = {}
    api_count_detail = {}

    enable = False

    @staticmethod
    def render(la):
        RenderErrCount.enable = True

        if 5 != len(la):
            return []

        service = Render.get_service_name(la[1])

        v = int(la[4])
        brief = la[3]
        tags = {
            'api': la[2],
            'service': service,
            'errcode': brief,
        }

        scd = Render.hash_default_get(RenderErrCount.service_count_detail, service, {})
        Render.hash_set_incr(scd, brief, v)
        RenderErrCount.service_count_detail[service] = scd

        return Render.pack(RenderErrCount.metric, tags, v)

    @staticmethod
    def _before_render(la):
        if 5 == len(la):
            service = Render.get_service_name(la[1])
            acd = Render.hash_default_get(RenderErrCount.api_count_detail, service, {})
            Render.hash_set_incr(acd, la[2], int(la[4]))
            RenderErrCount.api_count_detail[service] = acd

    @staticmethod
    def service_stat():
        res = []
        tags = { 'api': Render.reserved_service_name }

        for service, d in RenderErrCount.service_count_detail.items():
            tags['service'] = service
            for errcode, v in d.items():
                tags['errcode'] = errcode
                res.append(Render.pack(RenderErrCount.metric, tags, v))

        return res

    @staticmethod
    def _before_service_stat():

        for service, d in RenderErrCount.api_count_detail.items():
            sc = 0
            for api, v in d.items():
                sc += v
                RenderErrRate.push(key=RenderErrCount.metric, service=service, api=api, value=v)

            RenderErrRate.push(key=RenderErrCount.metric, service=service, api=Render.reserved_service_name, value=sc)

class RenderErrRate(Render):

    metric = 'error_rate'
    counts = {}
    enable = True

    @staticmethod
    def push(key, service, api, value):
        serv_hash = Render.hash_default_get(RenderErrRate.counts, service, {})
        api_hash = Render.hash_default_get(serv_hash, api, {})

        Render.hash_set_incr(api_hash, key, value)

        serv_hash[api] = api_hash
        RenderErrRate.counts[service] = serv_hash

    @staticmethod
    def service_stat():
        res = []

        for service, api_hash in RenderErrRate.counts.items():
            for api, d in api_hash.items():
                qc = Render.hash_default_get(d, RenderQueryCount.metric, 0)
                ec = Render.hash_default_get(d, RenderErrCount.metric, 0)
                total = qc + ec

                res.append(Render.pack(RenderErrRate.metric, {'api': api, 'service': service}, 0 if total == 0 else float(ec)/total))

        return res

class RenderLatency(Render):

    metric = 'latency_'
    service_latency = {}
    enable = False

    @staticmethod
    def __pack(tags, values):
        histo = Histogram(values)
        latencys = histo.percentiles()

        keys = ['50th', '75th', '95th', '99th']
        return map(lambda i: Render.pack(RenderLatency.metric+keys[i], tags, latencys[i]), range(4))

    @staticmethod
    def render(la):
        RenderLatency.enable = True

        if 4 != len(la):
            return []

        service = Render.get_service_name(la[1])
        sl = Render.hash_default_get(RenderLatency.service_latency, service, '')
        sl = sl + la[3]
        RenderLatency.service_latency[service] = sl

        return RenderLatency.__pack({ 'service': service, 'api': la[2] }, la[3])

    @staticmethod
    def service_stat():
        res = []

        for service, sl in RenderLatency.service_latency.items():
            res += RenderLatency.__pack({'service':service, 'api': Render.reserved_service_name}, sl)

        return res

class RenderDetailLatency(Render):

    service_latency = {}
    enable = False

    @staticmethod
    def render(la):
        RenderDetailLatency.enable = True

        if 4 != len(la):
            return []

        service = Render.get_service_name(la[1])

        item_obj = Render.hash_default_get(RenderDetailLatency.service_latency, la[0], {})
        ser_obj = Render.hash_default_get(item_obj, service, {'sum': 0, 'len': 0})
        ser_obj['sum'] += float(la[3])
        ser_obj['len'] += 1
        item_obj[service] = ser_obj
        RenderDetailLatency.service_latency[la[0]] = item_obj

        return Render.pack(la[0], { 'service': service, 'api': la[2] }, float(la[3]))

    @staticmethod
    def service_stat():
        res = []

        for item, item_obj in RenderDetailLatency.service_latency.items():
            for service, data in item_obj.items():
                res += (Render.pack(item, {'service': service, 'api': Render.reserved_service_name}, data['sum'] / data['len']), )

        RenderDetailLatency.service_latency = {}
        return res

class RenderUpstreamLatency(Render):

    metric = 'upstream_latency_'
    upstream_latency = {}
    enable = False

    @staticmethod
    def __pack(tags, values):
        histo = Histogram(values)
        latencys = histo.percentiles()

        keys = ['50th', '75th', '95th', '99th']
        return map(lambda i: Render.pack(RenderUpstreamLatency.metric+keys[i], tags, latencys[i]), range(4))

    @staticmethod
    def render(la):
        RenderUpstreamLatency.enable = True

        if 4 != len(la):
            return []

        service = Render.get_service_name(la[1])
        sl = Render.hash_default_get(RenderUpstreamLatency.upstream_latency, service, '')
        sl = sl + la[3]
        RenderUpstreamLatency.upstream_latency[service] = sl

        return RenderUpstreamLatency.__pack({'service': service, 'api': la[2]}, la[3])

    @staticmethod
    def service_stat():
        res = []

        for service, sl in RenderUpstreamLatency.upstream_latency.items():
            res += RenderUpstreamLatency.__pack({'service': service, 'api': Render.reserved_service_name}, sl)

        return res

renders = {
    'query_count': RenderQueryCount,
    'err_count': RenderErrCount,

    'latency': RenderLatency,
    'latency_50th': RenderDetailLatency,
    'latency_75th': RenderDetailLatency,
    'latency_95th': RenderDetailLatency,
    'latency_99th': RenderDetailLatency,

    'upstream_contacts': RenderUpstreamContacts,

    'upstream_latency': RenderUpstreamLatency,
    'upstream_latency_50th': RenderDetailLatency,
    'upstream_latency_75th': RenderDetailLatency,
    'upstream_latency_95th': RenderDetailLatency,
    'upstream_latency_99th': RenderDetailLatency,
}

derive_renders = {
    'err_rate': RenderErrRate,
}

def append_datapoint(datapoints, datapoint):
    if type(datapoint) == dict:
        datapoints.append(datapoint)
    elif type(datapoint) == list:
        datapoints += datapoint

def collect(url):
    global options
    datapoints = []

    try:
        content = requests.get(url).text
        print(content)
        ts = int(time.time())

        for line in content.splitlines():
            la = line.strip().split(options['ngx_out_sep'])
            append_datapoint(datapoints, Render.render(la))

        for key in renders.keys():
            append_datapoint(datapoints, Render.service_stat(renders[key]))

        for key in derive_renders.keys():
            append_datapoint(datapoints, Render.service_stat(derive_renders[key]))

        print(json.dumps(datapoints, indent=4, sort_keys=True))

        if options['format'] == 'falcon' and options['falcon_addr'] != '':
            r = requests.post(options['falcon_addr'], data=json.dumps(datapoints))
            print("push to falcon result: " + r.text)

        else:
            pass

    except Exception as e:
        traceback.print_exc(file = sys.stderr)

    sys.stdout.flush()
    sys.stderr.flush()

if __name__ == "__main__":

    config = ConfigParser()
    config.read('./config.ini')

    nginx_config = config['nginx']
    agent_config = config['agent']

    url = nginx_config['url']

    options = dict()
    keys = ['service', 'format', 'ngx_out_sep', 'falcon_addr', 'falcon_step']
    for key in keys:
        options.update({key: agent_config[key]})

    options['use_ngx_host'] = False
    options['falcon_step'] = int(options['falcon_step'])

    print(options)

    sys.exit(collect(url))
