import json
from py2neo import Graph, Relationship
import logging

#fp = open("mobile_list.json", "r")
fp = open("flipkart.json", "r")
graph = Graph("http://neo4j:asdf1234@localhost:7474/db/data")

with open('test.log', 'w'):
    pass

logging.basicConfig(format='%(asctime)s %(message)s',
                    filename='test.log', level=logging.WARNING)

#keys = ['brand', 'product_category', 'product_sub_category', 'product_rating']
keys = ['brand', 'product_rating']
#prod_prop = ['product_name', 'uniq_id', 'upc', 'retail_price, discounted_price']

data = json.load(fp)
#data = data[0:100]

# Create product node
def createProductNode(rec):
    logging.debug ("Creating product node")
    node = graph.merge_one('Product', 'product_name', rec['product_name'])
    node.properties['uniq_id'] = rec['uniq_id']
    return node

# Add product properties
def addNodeProperties(node, rec):
    if 'upc' in rec:
        node.properties['uniq_id'] = rec['uniq_id']
    if 'retail_price' in rec:
        node.properties['retail_price'] = rec['retail_price']
    if 'discounted_price' in rec:
        node.properties['discounted_price'] = rec['discounted_price']
    if 'product_review_count' in rec:
        node.properties['product_review_count'] = rec['product_review_count']
    logging.debug("Adding node properties")
    logging.debug(node.properties)

def extractAttribute(key, val):
    if(val == "Bluetooth"):
        return val, "Yes"
    elif ('RAM' in val):
        return 'RAM', val
    elif('Processor' in val):
        return 'CPU', val
    elif('Warranty' in val):
        return 'Warranty', val
    elif('Battery' in val):
        return 'Battery', val
    elif('Screen' in val):
        return 'Display', val
    elif('Display' in val):
        return 'Display', val
    elif('SIM' in val or 'GSM' in val or 'CDMA' in val):
        return 'SIM', val
    elif('Android' in val or 'Kitkat' in val or 'Jelly Bean' in val):
        return 'OS', val
    elif('Wi-Fi' in val):
        return 'Wi-Fi', val
    else:
        return key, val

def createAttributeNodes(node, i):
    #colorFound = False
    try:
        if(len(i) == 2):
            key = i['key']
            val = i['value']
            logging.debug("Specs: %s:%s", key, val)
            #if(key == "Color"):
                #colorFound = True
            """
            if(key == "info"):
                k,v = extractAttribute(key, val)
                logging.debug("Specs: Creating node %s:%s", k, v)
                attribute_node = graph.merge_one("Specs", k, v)
            else:
                logging.debug("Specs: Creating node %s:%s", key, val)
                attribute_node = graph.merge_one("Specs", key, val)
            """
            logging.debug("Specs: Creating node %s:%s", key, val)
            attribute_node = graph.merge_one("Specs", key, val)
            #Create relationship between product and attribute
            node_rel_dest = Relationship(node, "HAS", attribute_node)
            graph.create_unique(node_rel_dest)
    except Exception as e:
        print("Specs: Error parsing product specs:%s", i)
        print('Exception {} occurred {}'.format(e))

    """
    if (colorFound == False):
        prod_name = node.properties['product_name']
        color = utilities.searchPrefix(prod_name)
        #Create color node and relationship
        color_node = graph.merge_one('Color', 'Color', color)
        node_rel_dest = Relationship(node, "HAS_COLOR", color_node)
        graph.create_unique(node_rel_dest)
    """

# main

# Create nodes and relationship between category and sub-category

graph.delete_all()

parent_cat_node = graph.merge_one('Category', 'product_category', 'Mobiles & Tablets')
sub_cat_node = graph.merge_one('Category', 'product_sub_category', 'Mobile Phones')
node_rel_dest = Relationship(sub_cat_node, "SUB_CAT_OF", parent_cat_node)
graph.create_unique(node_rel_dest)

for d in data:
    rec = d['record']
    if not rec['product_name'] or not rec['uniq_id']:
        logging.info ("Incomplete product ... skipping")
        logging.debug(rec)
        continue
    else:
        node = createProductNode(rec)
        addNodeProperties(node, rec)
        node.push()
    
    for k,v in rec.iteritems():
        if k in keys:
            #Create relationship between product and sub-cat
            node_rel_dest = Relationship(node, "IS_A", parent_cat_node)
            graph.create_unique(node_rel_dest)
            node_rel_dest = Relationship(node, "IS_A", sub_cat_node)
            graph.create_unique(node_rel_dest)
            
            # Define relationships (product --> attribute)
            """
            if (k == 'product_category'):
                parent_cat = new_node
                rel = 'IS_A'
            elif (k == 'product_sub_category'):
                sub_cat = new_node
                rel = 'IS_A'
            """
            if (k == 'brand'):
                rel = k
            elif (k == 'product_rating'):
                rel = 'HAS_RATING'
            
            logging.debug("Creating node %s:%s", k, v)
            new_node = graph.merge_one(k, k, v)

            #Create relationship between product and attribute
            node_rel_dest = Relationship(node, rel, new_node)
            graph.create_unique(node_rel_dest)

        else:
            if (k == 'product_specifications'):
                specs = v['product_specification']
                #Product specifications
                for i in specs:
                    createAttributeNodes(node, i)


