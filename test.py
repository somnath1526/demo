import json
from elasticsearch import Elasticsearch ,helpers
from datetime import datetime
import requests
from elasticsearch_dsl import Search, Q, connections,A
import uuid
import time
import os
from urllib.parse import urlparse, parse_qs, unquote
import difflib
import base64
from dateutil.relativedelta import relativedelta
import xml.etree.ElementTree as ET
from env import consumer_key, consumer_secret




class EventScraper:
    def __init__(self, category_index, event_index, country_index, es_host,bearer_token):
        connections.create_connection(alias='default', hosts=[es_host])
        self.category_index = category_index
        self.event_index = event_index
        self.country_index = country_index
        self.event_meta_index='ticketwhiz_events_meta'
        self.soap_url = "https://www.tn-apis.com/TNWebservices/v4" 
        self.headers = {
                "accept": "text/xml",
                "SOAPAction": "http://tnwebservices.ticketnetwork.com/tnwebservice/v4.0/GetEvents",
                "Content-Type": "text/xml",
                "Authorization": f"Bearer {bearer_token}"
            }
        self.es = Elasticsearch(es_host)
        # self.index_name = index_name
        self.performer_index = 'ticketwhiz_performer'
        self.performer_meta_index = 'ticketwhiz_performer_meta'
        self.country_index = 'ticketwhiz_country'
        self.country = {}
        self.catgoryKey =[]
        self.catagory ={}
        self.bulk_event_data =[]
        self.bulk_event_meta_data =[]
        self.bulk_batch_size = 500  # you can adjust the batch size as needed
        self.geo_location = {}


    def callCountry(self):
        index_name = self.country_index
        response = Search(index=index_name).query("match_all")[0:10000].execute()
        result = {
            hit.country_id: (hit.Country_name, hit.abbreviation)
            for hit in response
            if hit.abbreviation and hit.Country_name and hit.country_id
        }
        return result

    def callCatagory(self):
        index_name = 'ticketwhiz_category'
        response = Search(index=index_name).query("match_all")[0:10000].execute()
        result = {
            hit.category_name: hit.category_id
            for hit in response
            if hit.category_name
        }
        self.catgoryKey = list(result.keys())
        return result

    def send_soap_request(self, date_time,end_date_time):
        try:
            data = f'''<?xml version="1.0" encoding="utf-8"?>
                <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                <soap:Body>
                    <GetEvents xmlns="http://tnwebservices.ticketnetwork.com/tnwebservice/v4.0">
                    <websiteConfigID>3551</websiteConfigID>
                    <beginDate>{date_time}</beginDate>
                    <endDate>{end_date_time}</endDate>
                    </GetEvents>
                </soap:Body>
                </soap:Envelope>'''

            response = requests.post(self.soap_url, headers=self.headers, data=data)
            return response.text
        except Exception as e:
            print(f"Error in main run function: {e}")
            return None

    def parse_soap_response(self, response_xml):
        try:
            if not response_xml:
                raise ValueError("No response data")

            namespaces = {'tn': "http://tnwebservices.ticketnetwork.com/tnwebservice/v4.0"}
            root = ET.fromstring(response_xml)

            for event in root.findall(".//tn:Event", namespaces):

                country_id = event.find('tn:CountryID', namespaces).text if event.find('tn:CountryID', namespaces) is not None else ''
                event_country = self.country.get(country_id)[0]
                eventData = {
                    "event_uuid": str(uuid.uuid4()),
                    "event_id": event.find("tn:ID", namespaces).text if event.find("tn:ID", namespaces) is not None else "",
                    "event_name": event.find("tn:Name", namespaces).text if event.find("tn:Name", namespaces) is not None else "",
                    "event_timing": event.find("tn:Date", namespaces).text if event.find("tn:Date", namespaces) is not None else "",
                    "event_display_time" : event.find("tn:DisplayDate", namespaces).text if event.find("tn:DisplayDate", namespaces) is not None else "",
                    "event_category": event.find("tn:ParentCategoryID", namespaces).text if event.find("tn:ParentCategoryID", namespaces) is not None else "",
                    "event_sub_category": event.find("tn:ChildCategoryID", namespaces).text if event.find("tn:ChildCategoryID", namespaces) is not None else "",
                    "event_grandChild_cata_id": event.find("tn:GrandchildCategoryID", namespaces).text if event.find("tn:GrandchildCategoryID", namespaces) is not None else "",
                    "event_location": f"{event.find('tn:Venue', namespaces).text if event.find('tn:Venue', namespaces) is not None else ''}, "
                                      f"{event.find('tn:City', namespaces).text if event.find('tn:City', namespaces) is not None else ''}, "
                                      f"{event.find('tn:StateProvince', namespaces).text if event.find('tn:StateProvince', namespaces) is not None else ''}, "
                                      f"{event_country}",
                    "event_venue_country": event.find("tn:CountryID", namespaces).text if event.find("tn:CountryID", namespaces) is not None else "",
                    'event_venue':f"{event.find('tn:Venue', namespaces).text if event.find('tn:Venue', namespaces) is not None else ''}",
                    'event_venue_id':f"{event.find('tn:VenueID', namespaces).text if event.find('tn:VenueID', namespaces) is not None else ''}",
                    'event_city': f"{event.find('tn:City', namespaces).text if event.find('tn:City', namespaces) is not None else ''}",
                    'event_state':f"{event.find('tn:StateProvince', namespaces).text if event.find('tn:StateProvince', namespaces) is not None else ''}",
                    "event_mapUrl": event.find("tn:MapURL", namespaces).text if event.find("tn:MapURL", namespaces) is not None else "",
                    "event_image": "",
                    "event_url": "",
                    "parent_id": ""
                }
                lat, lng = self.get_geo_location(eventData)
                print("*"*30)
                print( lat , lng)
                eventMain = {
                    '_op_type': 'index',
                    '_index': self.event_index,
                    '_source': {
                        'event_id': eventData['event_uuid'],
                        'event_name': eventData['event_name'],
                        'event_location': eventData['event_location'],
                        'event_venue': eventData['event_venue'],
                        'event_city': eventData['event_city'],
                        'event_state': eventData['event_state'],
                        'event_country': eventData['event_venue_country'],
                        'event_category': eventData['event_category'],
                        'event_sub_category': eventData['event_sub_category'],
                        'event_timing': eventData['event_timing'],
                        'event_image': '',
                        'location_point': {"lat": lat, "lon": lng},
                        'event_isTBA': 'false',
                        'created_at': datetime.now().replace(microsecond=0).isoformat()
                    }
                }
                # self.event_push_search(eventData)
                print(eventData)
                time.sleep(1)
        except ET.ParseError as e:
            print(f"Error parsing SOAP response: {e}")

    def get_geo_location(self,event_data):
        if event_data['event_venue_id'] in self.geo_location:
            lat = self.geo_location[event_data['event_venue_id']]['lat']
            lng = self.geo_location[event_data['event_venue_id']]['lng']
            print("get_geo_location from mapping")
            return lat , lng
        
        url = f"https://maps.googleapis.com/maps/api/geocode/json"
        api_key= "AIzaSyDvDugHW52jGDbaYXPzIgyOYlhFibEi0e8"
        params = {
            "address": event_data['event_location'],
            "key": api_key
        }

        response = requests.get(url, params=params)
        data = response.json()

        print("get_geo_location from google api call")
        

        if data['status'] == 'OK':
            result = data['results'][0]
            lat = result['geometry']['location']['lat']
            lng = result['geometry']['location']['lng']

            self.geo_location[event_data['event_venue_id']] = {'lat':lat, 'lng':lng}
            return lat, lng
        else:
            print("Error:", data['status'])
            return None, None
        

    def run(self):
        try:
            self.country = self.callCountry()
            self.catagory = self.callCatagory()

            beginning_date_time = datetime.now().isoformat()
            # end_date_time = (datetime.now()) + relativedelta(months=1) 
            end_date_time = (datetime.now()) + relativedelta(hours=5)
            end_date_time = end_date_time.isoformat()

            response_xml = self.send_soap_request(beginning_date_time,end_date_time)
            self.parse_soap_response(response_xml)
            
            # print(self.country)
            print(self.catagory)


            
        except Exception as e:
            print("Error in run method",e)



def get_bearer_token(consumer_key, consumer_secret):
    # -------------- generating bearer token ---------------
    consumer_key = consumer_key
    consumer_secret = consumer_secret

    # Encode to Base64
    credentials = f"{consumer_key}:{consumer_secret}"
    encoded_credentials = base64.b64encode(credentials.encode()).decode()


    # Set headers
    headers = {
        "Authorization": f"Basic {encoded_credentials}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # Set data
    data = {
        "grant_type": "client_credentials"
    }

    # Make the POST request
    response = requests.post("https://key-manager.tn-apis.com/oauth2/token", headers=headers, data=data, verify=False)

    

    response_json = response.json()

    bearer_token = response_json.get('access_token' ,'')

    print(bearer_token)
    return bearer_token

if __name__ == "__main__":

    bearer_token =  get_bearer_token(consumer_key, consumer_secret)
    scraper = EventScraper(es_host="http://192.168.2.44:9200", category_index="ticketwhiz_category", event_index="ticketwhiz_events", country_index="ticketwhiz_country",bearer_token=bearer_token)
    scraper.run()
