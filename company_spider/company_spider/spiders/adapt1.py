# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
import os
import csv
import glob
import mysql.connector
from scrapy.crawler import CrawlerProcess


class Adapt1Spider(scrapy.Spider):
    name = 'adapt1'
    allowed_domains = ['adapt.io']
    start_urls = ['https://www.adapt.io/directory/industry/telecommunications/A-1']

    def parse(self, response):
        sourse_url = response.xpath("//*[@class='DirectoryList_linkItemWrapper__3F2UE ']/a/@href").extract()
        for i in sourse_url:
            try:
               absolute_url = response.urljoin(i)
               yield Request(absolute_url,callback=self.parse_companies)
            except:
               pass 

        next_page_url =  response.xpath('//*[@class="DirectoryList_actionBtnLink__Seqhh undefined"]/a/@href').extract_first()
        yield Request(next_page_url)

    def parse_companies(self,response):
        
        Company_name = response.css('h1::text').extract_first()   
        Company_website = response.xpath('//*[@class="CompanyTopInfo_websiteUrl__13kpn"]/text()').extract_first()
        Company_industry = response.xpath('//*[@class="CompanyTopInfo_infoItem__2Ufq5"]//*[starts-with(.,"Industry")]/span/text()').extract()[1]
        response.xpath('//span[@class="CompanyTopInfo_infoValue__27_Yo"][3]/text()').extract()
        Company_revenue = response.xpath('//*[@class="CompanyTopInfo_infoItem__2Ufq5"]//*[starts-with(.,"Revenue")]/span/text()').extract()[1]
        Contact_jobtitle = response.xpath('//*[@class="TopContacts_jobTitle__3M7A2"]/text()').extract()
        Company_location = ''.join(response.xpath('//*[@class="CompanyTopInfo_contentWrapper__2Jkic"]//*[starts-with(.,"")]/span/text()').extract())
        Company_webdomain =  response.xpath('//*[@class="CompanyTopInfo_websiteUrl__13kpn"]/text()').extract_first()
        Company_employee_size = response.xpath('//*[@class="CompanyTopInfo_infoItem__2Ufq5"]//*[starts-with(.,"Head Count")]/span/text()').extract()[1]
        Contact_name= response.xpath('//*[@class="TopContacts_contactName__3N-_e"]/@content').extract()
        Contact_email_domain = response.xpath('//*[@class="simpleButton mailPhoneBtn emailBtn"]/text()').extract()

        

        yield{
        'Company_name':Company_name,
        'Company_website':Company_website,
        'Company_industry':Company_industry,
        'Company_revenue':Company_revenue,
        'Company_employee_size':Company_employee_size,
        'Company_location':Company_location,
        'Company_webdomain':Company_webdomain,
        'Contact_name':Contact_name,
        'Contact_jobtitle':Contact_jobtitle,
        'Contact_email_domain':Contact_email_domain
        }

    def close(self,reason):
        csv_file = max(glob.iglob('*.csv'),key=os.path.getctime)
        
        conn=mysql.connector.connect(host='localhost',user='root',passwd='root',db='company_db')

        cursor = conn.cursor()
        csv_data = csv.reader(file(csv_file))
        row_count = 0
        for row in csv_data:
            if row_count !=0:
                cursor.execute('insert ignore into company_details(Company_name,Company_industry,Company_website,Company_revenue,Company_employee_size,Company_location,Company_webdomain,Contact_name,Contact_jobtitle,Contact_email_domain) values(%s, %s,%s,%s,%s,%s,%s,%s,%s,%s)',row)
            row_count += 1
        conn.commit()
        cursor.close()
                
   

