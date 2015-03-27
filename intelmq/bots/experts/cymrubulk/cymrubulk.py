import json
from intelmq.lib.bot import Bot, sys
from intelmq.lib.cache import Cache
from intelmq.bots import utils
from intelmq.bots.experts.cymrubulk.lib import CymruBulk

from cymru.ip2asn.whois import WhoisClient as ip2asnWhois

MINIMUM_BGP_PREFIX_IPV4 = 24
MINIMUM_BGP_PREFIX_IPV6 = 128 # FIXME

class CymruBulkExpertBot(Bot):
    
    def init(self):
        self.cache = Cache(
                            self.parameters.redis_cache_host,
                            self.parameters.redis_cache_port,
                            self.parameters.redis_cache_db,
                            self.parameters.redis_cache_ttl
                          )

    
    def process(self):
        events = self.reserve(self.parameters.maxchunk)
        
        self.ipList = [] 
        for event in events:
            keys = ["source_%s", "destination_%s"]
            for key in keys:
                ip = event.value(key % "ip")
                self.ipList.append(ip)
        
        # Do the Cymru query, get dictionary of results.
        ipSet = set(ipList)
        cymruClient = ip2asnWhois()
        cymruResult = cymruClient.lookupmany(ipSet, qType)
        
        for event in events:
            keys = ["source_%s", "destination_%s"]
            for key in keys:
                ip = event.value(key % "ip")
                if not cymruResult[ip]:
                    continue
                
                result = cymruResult[ip]
            
                if "asn" in result:
                    event.clear(key % 'asn')
                    event.add(key % 'asn',        result['asn'])
                    
                if "prefix" in result:
                    event.clear(key % 'bgp_prefix')
                    event.add(key % 'bgp_prefix', result['bgp_prefix'])
                    
                if "lir" in result:
                    event.clear(key % 'registry')
                    event.add(key % 'registry',   result['registry'])
                    
                if "date" in result:
                    event.clear(key % 'allocated')
                    event.add(key % 'allocated',  result['allocated'])
                    
                if "owner" in result:
                    event.clear(key % 'as_name')
                    event.add(key % 'as_name',    result['as_name'])
                    
                if "cc" in result:
                    event.clear(key % 'cymrubulk_cc')
                    event.add(key % 'cymrubulk_cc',   result['cc'])

        # TODO: Should have more granularity, error management here.
        self.release()
        
if __name__ == "__main__":
    bot = CymruBulkExpertBot(sys.argv[1])
    bot.start()
