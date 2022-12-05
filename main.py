from notion_client import Client
from notion2md.exporter.block import MarkdownExporter, StringExporter
from dotenv import load_dotenv
import json
import pandas as pd
import numpy as np
import os

class BP_CVE_Notion:
    def __init__(self):
        load_dotenv()
        notion_api = os.environ.get("NOTION_TOKEN")
        self.client = Client(auth=notion_api)
        self.databaseId = os.environ.get("DATABASE_ID")
        self.bp_notion = None
        
    def get_notion_db(self):
        bp_cve = self.client.databases.query(
            **{
                "database_id":self.databaseId,
                "filter":{
                    "or":[
                        {
                            "property": "상태",
                            "status":{
                                "equals" : "Done"
                            }
                        },
                        {
                            "property": "상태",
                            "status":{
                                "equals" : "Checking"
                            }
                        },
                        {
                            "property": "상태",
                            "status":{
                                "equals" : "Not Regex"
                            }
                        },
                        {
                            "property": "상태",
                            "status":{
                                "equals" : "Updated"
                            }
                        }
                    ] 
                },
                "sorts": [
                    {
                        "property":"날짜",
                        "direction":"ascending"
                    },
                    {
                        "property":"이름",
                        "direction":"ascending"
                    }
                ]
            }
        )
        df = pd.DataFrame(bp_cve['results'])
        df = df[['id', 'properties']]
        df['Name'] = df['properties'].apply(lambda x: x['이름']['title'][0]['plain_text'])
        df = df.sort_values('Name').reset_index(drop=True)[['Name', 'id', 'properties']]
        print(df)
        self.bp_notion = df.copy()
        return
        


def main():
    gnd = BP_CVE_Notion()
    gnd.get_notion_db()
    return


if __name__ == "__main__":
    main()