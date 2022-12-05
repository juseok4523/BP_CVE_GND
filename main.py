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
        self.notion_data = None
        self.local_data = None
        
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
        df['Status'] = df['properties'].apply(lambda x: x['상태']['status']['name'])
        df['StartDate'] = df['properties'].apply(lambda x: x['날짜']['date']['start'])
        df['EndDate'] = df['properties'].apply(lambda x: x['날짜']['date']['end'])
        
        df = df.sort_values('Name').reset_index(drop=True)[['Name', 'id', 'Status','StartDate', 'EndDate']]
        print(df)
        self.notion_data = df.copy()
        return
        
    def compare_notion(self):
        if self.local_data == None:
            pass
        


def main():
    gnd = BP_CVE_Notion()
    gnd.get_notion_db()
    return


if __name__ == "__main__":
    main()