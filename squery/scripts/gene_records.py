from SPARQLWrapper import SPARQLWrapper, JSON
import pprint
import pandas as pd
import psycopg2
import sys
from sqlalchemy import create_engine


def run(*taxid):
    """
    sparql query to return genes by species with genome coordinates
    :return: .csv
    """
    endpoint = SPARQLWrapper("https://query.wikidata.org/bigdata/namespace/wdq/sparql")
    wd = 'PREFIX wd: <http://www.wikidata.org/entity/>'
    wdt = 'PREFIX wdt: <http://www.wikidata.org/prop/direct/>'

    def execute_query():
        query = '''SELECT  ?specieswd ?specieswdLabel ?taxid ?genomeaccession ?geneLabel ?locustag ?entrezid ?genomicstart ?genomicend
                    WHERE {{
                    ?specieswd wdt:P685 "{}".
                    ?specieswd wdt:P685 ?taxid;
                    	wdt:P2249 ?genomeaccession.
                    ?gene wdt:P703 ?specieswd;
                          wdt:P351 ?entrezid ;
               			  wdt:P644 ?genomicstart;
                          wdt:P645 ?genomicend;
                          wdt:P2393 ?locustag;

                    SERVICE wikibase:label {{
    					bd:serviceParam wikibase:language "en" .
  					}}

                    }}
                '''.format(taxid[0])

        endpoint.setQuery(query)
        endpoint.setReturnFormat(JSON)
        wd_query = endpoint.query().convert()
        sparql_res = wd_query['results']['bindings']

        items_list = []
        for i in sparql_res:
            idict = {'entrezid': i['entrezid']['value'],
                     'specieswd': i['specieswd']['value'],
                     'taxid': i['taxid']['value'],
                     'organismname': i['specieswdLabel']['value'],
                     'genomeaccession': i['genomeaccession']['value'],
                     'locustag': i['locustag']['value'],
                     'genomicstart': i['genomicstart']['value'],
                     'genomicend': i['genomicend']['value'],
                     'genesymbol': i['geneLabel']['value'],

                     }
            items_list.append(idict)

        return pd.DataFrame(items_list)

    genes = execute_query()
    df_joined = genes[['entrezid' ,'specieswd' ,'taxid', 'organismname' ,'genomeaccession' ,'locustag' ,'genomicstart' ,'genomicend' ,'genesymbol']]

    sparql_genes = df_joined.to_json(orient='records')

    engine = create_engine('postgresql://timputman:pass@localhost/dj_microbes')
    df_joined.to_sql('genomecoords', engine, if_exists='replace')

    pprint.pprint( sparql_genes)



