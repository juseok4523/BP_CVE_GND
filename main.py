from notion_client import Client
from notion2md.exporter.block import MarkdownExporter, StringExporter
from dotenv import load_dotenv
import json
import pandas as pd
import numpy as np
import os
from notion2md.exporter.block import MarkdownExporter, StringExporter
import multiprocessing as mp

class BP_CVE_Notion:
    def __init__(self):
        load_dotenv()
        notion_api = os.environ.get("NOTION_TOKEN")
        self.client = Client(auth=notion_api)
        self.databaseId = os.environ.get("DATABASE_ID")
        self.notion_data = None
        self.local_data = None
        self.compare_df = None
        self.save_dir = './BP-CVE-Data/BP_CVE'
        
        
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
                    }
                }
        )
        while bp_cve['has_more']:
            next_data = self.client.databases.query(
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
                    "start_cursor":bp_cve['next_cursor']
                }
            )
            bp_cve['results'].extend(next_data['results'])
            bp_cve['has_more'] = next_data['has_more']
            bp_cve['next_cursor'] = next_data['next_cursor']

        df = pd.DataFrame(bp_cve['results'])
        df = df[['id', 'properties']]
        df['Name'] = df['properties'].apply(lambda x: x['이름']['title'][0]['plain_text'])
        df['Status'] = df['properties'].apply(lambda x: x['상태']['status']['name'])
        df['StartDate'] = df['properties'].apply(lambda x: x['날짜']['date']['start'])
        df['EndDate'] = df['properties'].apply(lambda x: x['날짜']['date']['end'])
        
        df = df.sort_values('Name').reset_index(drop=True)[['Name', 'id', 'Status','StartDate', 'EndDate']]
        self.notion_data = df.copy()
        return
    
    def compare_notion(self):
        if self.local_data is not None:
            if not self.local_data.equals(self.notion_data):
                print('Update Local Data')
                merge_df = pd.merge(self.notion_data, self.local_data, how='outer', on=['Name','id'])
                compare_df = merge_df[merge_df['Status_x'] != merge_df['Status_y']]
                compare_df = compare_df[['Name','id', 'Status_x', 'StartDate_x', 'EndDate_x']].rename(columns={'Status_x':'Status', 'StartDate_x':'StartDate', 'EndDate_x':'EndDate'}).dropna(axis=0)
                if compare_df is not None:
                    self.local_data = pd.concat([self.local_data, compare_df], axis=0, join='outer')
                    self.compare_df = compare_df.copy()
                print(compare_df)
            else :
                print('Not Update Local Data')
        else:
            print('Init Local Data')
            self.compare_df = self.notion_data.copy()
            self.local_data = self.notion_data.copy()
        return
            
    def get_local_data(self, path='./BP-CVE-Data/', filename='bp_cve_list.csv'):
        if os.path.isfile(path+filename) :
            self.local_data = pd.read_csv(path+filename, index_col = 0)
        return
    
    def save_local_data_df(self, path='./BP-CVE-Data/', filename='bp_cve_list.csv'):
        self.local_data.to_csv(path+filename)
        return
        
    def change_notion_md(self):
        print('Write Notion with Md...')
        temp_df = self.compare_df.copy()
        temp_df.apply(lambda row: MarkdownExporter(block_id=row['id'], output_path=self.save_dir, output_filename=row['Name'], unzipped=True, download=True).export(), axis=1)
        return

    def update_result(self):
        print('Update Result Board...')
        mdfile = MdUtils(file_name='BP-CVE-Data/BP-CVE_Result')
        #karban-plugin
        mdfile.write("---\n\nkanban-plugin: basic\n\n---\n")
        
        #Done
        mdfile.write('\n## Done\n')
        done_df = self.local_data[self.local_data['Status'] == 'Done'].copy()
        done_df['Md_Name'] = done_df['Name'].apply(lambda x: f'[[{x}]]')
        mdfile.new_checkbox_list(done_df['Md_Name'])
        
        #Checking
        mdfile.write('\n## Checking\n')
        check_df = self.local_data[self.local_data['Status'] == 'Checking'].copy()
        check_df['Md_Name'] = check_df['Name'].apply(lambda x: f'[[{x}]]')
        mdfile.new_checkbox_list(check_df['Md_Name'])
        
        #Not Regex
        mdfile.write('\n## Not Regex\n')
        not_df = self.local_data[self.local_data['Status'] == 'Not Regex'].copy()
        not_df['Md_Name'] = not_df['Name'].apply(lambda x: f'[[{x}]]')
        mdfile.new_checkbox_list(not_df['Md_Name'])
        
        #Updated
        mdfile.write('\n## Updated\n')
        update_df = self.local_data[self.local_data['Status'] == 'Updated'].copy()
        update_df['Md_Name'] = update_df['Name'].apply(lambda x: f'[[{x}]]')
        mdfile.new_checkbox_list(update_df['Md_Name'])
        
        #karban-setting
        mdfile.new_paragraph("%% kanban:settings")
        mdfile.write('\n```\n{"kanban-plugin":"basic"}\n```\n')
        mdfile.write("%%")
        mdfile.create_md_file()

def main():
    gnd = BP_CVE_Notion()
    gnd.get_notion_db()
    gnd.get_local_data()
    gnd.compare_notion()
    if gnd.compare_df is not None:
        gnd.change_notion_md()
        gnd.save_local_data_df()
    gnd.update_result()
    return


if __name__ == "__main__":
    main()