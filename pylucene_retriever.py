import sys
import lucene
 
from java.io import File
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.document import Document, Field
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.index import IndexReader
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.util import Version
 
if __name__ == "__main__":
    lucene.initVM()
    print "lucene version is:", Version
    # Get the analyzer
    analyzer = StandardAnalyzer(Version.LUCENE_CURRENT)

    # Get index storage
    indexDir = SimpleFSDirectory(File("index/"))
    reader = IndexReader.open(SimpleFSDirectory(File("index/")))
    searcher = IndexSearcher(reader)
 
    query = QueryParser(Version.LUCENE_CURRENT, "country", analyzer).parse("India")
    MAX = 1000
    hits = searcher.search(query, MAX)
 
    print "Found %d document(s) that matched query '%s':" % (hits.totalHits, query)
    for hit in hits.scoreDocs:
        print hit.score, hit.doc, hit.toString()
        doc = searcher.doc(hit.doc)
        print doc.get("country").encode("utf-8")
