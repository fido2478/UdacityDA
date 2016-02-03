#!/usr/bin/env python
# -*- coding: utf-8 -*-
import codecs
import json
import re
import xml.etree.ElementTree as ET
from collections import defaultdict
import pprint

from pymongo import MongoClient

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
street_type_re = re.compile(r'\b\S+\.?$', re.IGNORECASE)
pc_type_re1 = re.compile(r'[a-z]')
pc_type_re2 = re.compile(r'[-]')

expected = ["Street", "Avenue", "Boulevard", "Drive", "Court", "Place", "Square", "Lane", "Road",
            "Trail", "Parkway", "Commons"]
mapping = { "St": "Street",
            "St.": "Street",
            "Ave": "Avenue",
            "Rd.": "Road",
            "Rd": "Road",
            "Ln": "Lane",
            "W.": "West",
            "W": "West",
            "N.": "North",
            "N": "North",
            "S.": "South",
            "S": "South",
            "E": "East",
            "E.": "East",
            "Ct": "Center",
            "Ct.": "Center",
            "Dr": "Drive",
            "Dr.": "Drive",
            "Cir": "Circle",
            "Cir.": "Circle",
            "Rte": "Route",
            "Rte.": "Route",
            "Blvd": "Boulevard",
            "Blvd.": "Boulevard",
            "Pkwy": "Parkway",
            "Ste": "Suite"
            }
CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

"""
check the "k" value for each "<tag>" and see if they can be valid keys in MongoDB, as well as
see if there are any other potential problems.
expand the "addr:street" type of keys to a dictionary like this:
{"address": {"street": "Some value"}}

four tag categories in a dictionary:
  "lower", for tags that contain only lowercase letters and are valid,
  "lower_colon", for otherwise valid tags with a colon in their names,
  "problemchars", for tags with problematic characters, and
  "other", for other tags that do not fall into the other three categories.
"""
def key_type(element, keys):
    if element.tag == "tag":
        k_value = element.attrib['k']
        if lower.search(k_value) is not None:
            keys['lower'] += 1
        elif lower_colon.search(k_value) is not None:
            keys['lower_colon'] += 1
        elif problemchars.search(k_value) is not None:
            keys["problemchars"] += 1
        else:
            keys['other'] += 1

    return keys


"""
audit
- audit the OSMFILE and change the variable 'mapping' to reflect the changes needed to
fix the unexpected street types to the appropriate ones in the expected list.
- fix the street name.
"""
def is_street_name(elem):
    return (elem.attrib['k'] == "addr:street")

def is_postcode(elem):
    return (elem.attrib['k'] == "addr:postcode")

def update_name(name, mapping):

    after=[]
    namearr=name.split(" ")
    for name in namearr:
        for key in mapping.keys():
            if name==key:
                name=mapping[key]
                break
        after.append(name)
    return " ".join(after)

"""
wrangle the data and transform the shape of the data into the model
- all attributes of "node" and "way" should be turned into regular key/value pairs, except:
- attributes in the CREATED array should be added under a key "created"
- attributes for latitude and longitude should be added to a "pos" array, for use
  in geospacial indexing. Make sure the values inside "pos" array are floats and not strings.
- if second level tag "k" value contains problematic characters, it should be ignored
- if second level tag "k" value starts with "addr:", it should be added to a dictionary "address"
- if second level tag "k" value does not start with "addr:", but contains ":", you can process it
  same as any other tag.
- if there is a second ":" that separates the type/direction of a street,
  the tag should be ignored
"""
def is_address(elem):
    if elem.attrib['k'][:5] == "addr:":
        return True

def shape_element(street_types, element):
    node = {}
    if element.tag == "node" or element.tag == "way":
        address_info = {}
        nd_info = []
        #pprint.pprint(element.attrib)
        node["type"] = element.tag
        node["id"] = element.attrib["id"]
        if "visible" in element.attrib.keys():
            node["visible"] = element.attrib["visible"]
        if "lat" in element.attrib.keys():
            node["pos"] = [float(element.attrib['lat']), float(element.attrib['lon'])]
        node["created"]={}
        if "version" in element.attrib.keys():
            node["created"]["version"]= element.attrib['version']
        if "changeset" in element.attrib.keys():
            node["created"]["changeset"]= element.attrib['changeset']
        if "timestamp" in element.attrib.keys():
            node["created"]["timestamp"]= element.attrib['timestamp']
        if "uid" in element.attrib.keys():
            node["created"]["uid"]=element.attrib['uid']
        if "user" in element.attrib.keys():
            node["created"]["user"] = element.attrib['user']
        for tag in element.iter("tag"):
            tag_at_v=tag.attrib['v']
            p = problemchars.search(tag.attrib['k'])
            if p:
                print "PROBLEM:", p.group()
                continue
            elif is_address(tag):
                if is_street_name(tag): #match addr:street
                    m = street_type_re.search(tag_at_v)
                    if m:
                        street_type = m.group()
                        if street_type not in expected:
                            #street_types[street_type].add(tag_at_v)
                            tag_at_v=update_name(tag_at_v, mapping) #spell it out
                elif is_postcode(tag): #match addr:postcode
                    m = pc_type_re2.search(tag_at_v)
                    if m: # 95004-9610 --> 95004
                        tag_at_v=tag_at_v[:5]
                    else:
                        m = pc_type_re1.search(tag_at_v)
                        if m: # 411 Cole Rd, Aromas, CA 95004  -->  95004
                            tag_at_v=tag_at_v[len(tag_at_v)-5:]

                address_info[tag.attrib['k'][5:]]=tag_at_v
            else:
                node[tag.attrib['k']] = tag_at_v
        if address_info != {}:
            node['address'] = address_info
        for tag2 in element.iter("nd"):
            nd_info.append(tag2.attrib['ref'])
        if nd_info != []:
            node['node_refs'] = nd_info
        return node
    else:
        return None

"""
find out not only what tags are there, but also how many, to get the
feeling on how much of which data you can expect to have in the map.
"""
def count_tags(filename):
    counts = defaultdict(int)
    for line in ET.iterparse(filename):
        #print line[1]
        current = line[1].tag
        counts[current] += 1
    return counts

def process_map(file_in, pretty = False):
    file_out = "{0}.json".format(file_in)
    keys = {"lower": 0, "lower_colon": 0, "problemchars": 0, "other": 0} # 1.
    users = set() # 2.
    data = [] # 3.
    street_types = defaultdict(set)
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            # 1. check the "k" value for each "<tag>" and  see if they can be valid keys
            keys = key_type(element, keys)
            # 2. audit, wrangle the data and transform the shape of the data
            el = shape_element(street_types, element)
            if el:
                data.append(el)
        fo.write(json.dumps(data))
    return keys, street_types

def get_db(db_name):
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db

def insert_data(dbcol, data):
    # Your code here. Insert the data into a collection 'arachnid'
    dbcol.insert(data)
    pass

def aggregate(dbcol, pipeline):
    result = dbcol.aggregate(pipeline)
    return result

def test():
    filename='by2-sf-bay-area.osm'
    #This is for statistics of tags
    #tags=count_tags(filename)
    #pprint.pprint(tags)

    # audit, wrangle data and transform osm to json
    keys, st_types = process_map(filename, False)
    pprint.pprint(keys)
    if keys["problemchars"] > 0:
        print "There is (are) ", keys["problemchars"],  " problematic value of k"
    #print "The number of abbreviated streets is ", len(st_types)
    #pprint.pprint(dict(st_types))

    db = get_db('by2_sf_bay')
    dbcol=db.by2_sf_bay

    # open json and insert it into MangoDB
    with open(filename+'.json') as f:
        data = json.loads(f.read())
        insert_data(dbcol,data)
        print dbcol.find_one()

    # Statistcs of tags
    print "Number of documents: " , dbcol.find().count()
    print "Number of nodes: " , dbcol.find({"type":"node"}).count()
    print "Number of ways: " , dbcol.find({"type":"way"}).count()

    # Check the correctness of postcodes and city names
    pl = [{"$match":{"address.postcode":{"$exists":1}}},
          {"$group":{"_id":"$address.postcode", "count":{"$sum":1}}}, {"$sort":{"count":1}}]
    print "List of Postcodes: ",list(aggregate(dbcol,pl))
    pl = [{"$match":{"address.city":{"$exists":1}}},
          {"$group":{"_id":"$address.city", "count":{"$sum":1}}}, {"$sort":{"count":1}}]
    print "Cities: ",list(aggregate(dbcol,pl))

    # get contributors
    print "Number of unique users: " , len(dbcol.distinct("created.user"))
    pl= [{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
         {"$sort":{"count":-1}}, {"$limit":1}]
    print "Top 1 contributing user: ", list(aggregate(dbcol,pl))
    pl= [{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
         {"$sort":{"count":-1}}, {"$limit":2}]
    print "Top 2 contributing user: ", list(aggregate(dbcol,pl))
    pl= [{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
         {"$sort":{"count":-1}}, {"$limit":10}]
    print "Top 10 contributing user: ", list(aggregate(dbcol,pl))

    # explore user data
    pl=[{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
        {"$group":{"_id":"$count", "num_users":{"$sum":1}}},
        {"$sort":{"_id":1}}, {"$limit":1}]
    print "Number of users appearing only once (having 1 post): ", list(aggregate(dbcol,pl))
    pl=[{"$group":{"_id":"$created.user", "count":{"$sum":1}}},
        {"$group":{"_id":"$count", "num_users":{"$sum":1}}},
        {"$sort":{"_id":1}}, {"$limit":1}]
    print "Number of users appearing only 1% of posts: ", list(aggregate(dbcol,pl))

    # explore amenity data
    pl = [{"$match":{"amenity":{"$exists":1}}}, {"$group":{"_id":"$amenity",
"count":{"$sum":1}}}, {"$sort":{"count":-1}}, {"$limit":10}]
    print "Top 10 appearing amenities: ", list(aggregate(dbcol,pl))
    pl = [{"$match":{"amenity":{"$exists":1}, "amenity":"place_of_worship"}},
          {"$group":{"_id":"$religion", "count":{"$sum":1}}},
          {"$sort":{"count":-1}}, {"$limit":1}]
    print "Biggest religion (no surprise here): ",list(aggregate(dbcol,pl))
    pl = [{"$match":{"amenity":{"$exists":1}, "amenity":"restaurant","cuisine":{"$exists":1}}},
          {"$group":{"_id":"$cuisine", "count":{"$sum":1}}},
          {"$sort":{"count":-1}}, {"$limit":2}]
    print "Most popular cuisines: ",list(aggregate(dbcol,pl))

if __name__ == "__main__":
    test()