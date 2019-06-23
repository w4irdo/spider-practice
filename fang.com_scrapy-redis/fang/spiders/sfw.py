# -*- coding: utf-8 -*-
import scrapy
import re
from fang.items import NewHouseItem, ESFHouseItem
from scrapy_redis.spiders import RedisSpider


class SfwSpider(RedisSpider):
    name = 'sfw'
    allowed_domains = ['fang.com']
    # start_urls = ['https://www.fang.com/SoufunFamily.htm']
    redis_key = "fang:start_urls"
    
    def parse(self, response):
        trs = response.xpath("//div[@class='outCont']//tr")
        province = None
        for tr in trs:
            tds = tr.xpath(".//td[not(@class)]")
            province_td = tds[0]
            province_text = province_td.xpath(".//text()").get()
            province_text = re.sub(r"\s", "", province_text)
            if province_text:
                province = province_text
            if province == "其它":
                continue
            city_links = tds[1].xpath(".//a")
            for city_link in city_links:
                city = city_link.xpath(".//text()").get()
                city_url = city_link.xpath(".//@href").get()
                url_module = city_url.split("//")
                scheme = url_module[0]
                domain = url_module[1]
                if "bj" in domain:
                    newhouse_url = "https://newhouse.fang.com/house/s/"
                    esf_url = "https://esf.fang.com/"
                else:
                    newhouse_url = scheme + "//" + "newhouse." + domain + "house/s"
                    esf_url = scheme + "//" + "esf." + domain
                print("城市: %s%s" % (province, city))
                print("新房链接:%s " % newhouse_url)
                print("二手房链接：%s " % esf_url)
                
                yield scrapy.Request(url=newhouse_url, callback=self.parse_newhouse,
                                     meta={'info': (province, city)})
                yield scrapy.Request(url=esf_url, callback=self.parse_esf, meta={'info': (
                    province, city)})
               
            
        
    def parse_newhouse(self, response):
        province, city = response.meta.get("info")
        lis = response.xpath("//div[contains(@class, 'nl_con')]/ul/li")
        for li in lis:
            name = "".join(li.xpath(".//div[@class='nlcd_name']//text()").getall())
            name = re.sub(r'\s', '', name)
            house_type_list = "".join(li.xpath(".//div[contains(@class,'house_type')]//text("
                                               ")").getall())
            house_type_list = re.sub(r'\s', "", house_type_list)
            if "居" not in house_type_list:
                continue
            rooms = house_type_list.split("－")[0]
            area = house_type_list.split("－")[1]
            address = li.xpath(".//div[@class='address']/a/@title").get()
            district = "".join(li.xpath(".//div[@class='address']/a//text()").getall())
            district = re.search(r'\[(.+)\].*', district).group(1)
            sale = li.xpath(".//span[@class='inSale']/text()").get()
            origin_url = "https:" + li.xpath(".//div[@class='nlcd_name']/a/@href").get()
            price = "".join(li.xpath(".//div[@class='nhouse_price']//text()").getall()).strip()
            price = re.sub(r'\s|广告', "", price)
            item = NewHouseItem(name=name, rooms=rooms, area=area, address=address,
                                district=district, sale=sale, origin_url=origin_url, price=price,
                                province=province, city=city)
            yield item
        next_url = response.xpath(".//li[@class='fr']/a[@class='next']/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_newhouse,
                                 meta={"info": (province, city)})
        
    def parse_esf(self, response):
        province, city = response.meta.get("info")
        dls = response.xpath("//div[contains(@class, 'shop_list')]/dl")
        for dl in dls:
            item = ESFHouseItem(province=province, city=city)
            name = dl.xpath(".//span[@class='tit_shop']/text()").get()
            if not name:
                continue
            infos = "".join(dl.xpath(".//p[@class='tel_shop']//text()").getall()).strip()
            infos = infos.split("|")
            if infos[0] == "独栋":
                item['rooms'] = infos[1].strip() + "[别墅]"
                item['area'] = infos[3].strip()
                item['floor'] = infos[2].strip()
                item['toward'] = infos[4].strip()
            else:
                item['rooms'] = infos[0].strip()
                item['area'] = infos[1].strip()
                item['floor'] = infos[2].strip()
                item['toward'] = infos[3].strip()
                try:
                    item['year'] = infos[4].strip()
                except IndexError:
                    print("没有年份记录")
            address = dl.xpath(".//p[@class='add_shop']/span/text()").get()
            item['address'] = address
            price_text = "".join(dl.xpath(".//dd[@class='price_right']//text()").getall()).strip()
            price_text = re.sub(r'\s', '', price_text)
            price = price_text.split("万")[0].strip()+ "万"
            unit = price_text.split("万")[1].strip()
            item['price'] = price
            item['unit'] = unit
            origin_url_text = dl.xpath(".//h4[@class='clearfix']/a/@href").get()
            origin_url = response.urljoin(origin_url_text)
            item['origin_url'] = origin_url
            yield item
        next_url = response.xpath(".//div[@class='page_al']/p[1]/a/@href").get()
        if next_url:
            yield scrapy.Request(url=response.urljoin(next_url), callback=self.parse_esf,
                                 meta={"info": (province, city)})
            
            
