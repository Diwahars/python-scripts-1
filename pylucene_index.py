import lucene
import json
import logging
 
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field
from org.apache.lucene.index import IndexWriter, IndexWriterConfig
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version

INDEX_DIR='index/'
INP_FILE = "flipkart.json"
keys = ['brand', 'product_rating']

if __name__ == "__main__":
    fp = open(INP_FILE, "r")
    data = json.load(fp)
    data = data[0:10]
    

    lucene.initVM()
  
    print "lucene version is:", lucene.VERSION
    # Get the analyzer
    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)

    # Get index storage
    indexDir = SimpleFSDirectory(File("index/"))

    # Get index writer
    config = IndexWriterConfig(Version.LUCENE_CURRENT, analyzer);
    writer = IndexWriter(indexDir, config);

    print "%d docs in index" % writer.numDocs()

    for d in data:
        rec = d['record']
        if not rec['product_name'] or not rec['uniq_id']:
            logging.info ("Incomplete product ... skipping")
            logging.debug(rec)
            continue
        else:
            doc = Document()
            for k,v in rec.iteritems():
                if k in keys:
                    doc.add(Field(k, v, Field.Store.YES, Field.Index.ANALYZED))
                else:
                    if (k == 'product_specifications'):
                        specs = v['product_specification']
                        #Product specifications
                        for i in specs:
                            try:
                                if(len(i) == 2):
                                    key = i['key']
                                    val = i['value']
                                    logging.debug("Specs: %s:%s", key, val)
                                    doc.add(Field(k, v, Field.Store.YES, Field.Index.ANALYZED))
                            except Exception as e:
                                continue
                                logging.error("Specs: Error parsing product specs:%s", i)
                                logging.error('Exception {} occurred {}'.format(e))

        writer.addDocument(doc)

    print "Closing index of %d docs..." % writer.numDocs()
    writer.close()


