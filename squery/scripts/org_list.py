__author__ = 'timputman'

from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from sqlalchemy import create_engine


def run():
    def sparql_for_qids():
        """
        sparql query to return number of genes and proteins by species with unique taxid in WD
        :return: .csv
        """
        endpoint = SPARQLWrapper("https://query.wikidata.org/bigdata/namespace/wdq/sparql")

        def execute_query():
            query = '''SELECT ?species ?speciesLabel ?taxid ?RefSeq
                    WHERE {
                        ?species wdt:P171* wd:Q10876;
                        wdt:P685 ?taxid;
                        wdt:P2249 ?RefSeq.
                        SERVICE wikibase:label {
                        bd:serviceParam wikibase:language "en" .
                            }
                        }
                    '''

            endpoint.setQuery(query)
            endpoint.setReturnFormat(JSON)
            wd_query = endpoint.query().convert()
            return wd_query['results']['bindings']

        def sparql2pandas(sparql):
            items_list = []
            for i in sparql:
                idict = {'wd_species': i['species']['value'],
                         'taxid': i['taxid']['value'],
                         'label': i['speciesLabel']['value'],
                         'refseq': i['RefSeq']['value'],
                         }
                items_list.append(idict)

            return pd.DataFrame(items_list)

        zquery = execute_query()
        pdquery = sparql2pandas(zquery)
        engine = create_engine('postgresql://timputman:pass@localhost/dj_microbes')

        pdquery.to_sql('orglist', engine, if_exists='replace')
        if not pdquery.empty:
            print('Organism selector list is updated!')
    return sparql_for_qids()



