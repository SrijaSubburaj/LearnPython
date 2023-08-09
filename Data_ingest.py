from azure.core.exceptions import HttpResponseError, ResourceExistsError
from azure.storage.blob import BlobServiceClient 
from azure.identity import DefaultAzureCredential
from dotenv import load_dotenv
import os
import yaml
import pandas as pd

def data_ingestion():

    # Accessing the azure storage account
    load_dotenv("Credentials_azure1.env")
    token_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(account_url="https://demolpdblob.blob.core.windows.net",credential=token_credential)

    # Creating a container for logs without meta
    try:
        container_client1 = blob_service_client.create_container(name="01-ingest-logs")
        container_client2 = blob_service_client.create_container(name="01-ingest-meta")
    except ResourceExistsError:
        print('Containers with these names already exists')
   
    # Download user-info file
    blob_client_instance1 = blob_service_client.get_blob_client("user-info", "User_info.xlsx", snapshot=None)
    with open("User_info.xlsx", "wb") as my_blob1:
        blob_data1 = blob_client_instance1.download_blob()
        blob_data1.readinto(my_blob1)

    # reading the downloaded user-info file
    user_info = pd.read_excel("User_info.xlsx")
    user_info.set_index('User',inplace=True)
    os.remove("User_info.xlsx") # removing the user_info file

    # fetching the list of blobs
    container_client3 = blob_service_client.get_container_client(container="source")
    blob_list = container_client3.list_blobs()
    for each in blob_list:
        
        # Download blob file
        blob_client_instance = blob_service_client.get_blob_client("source", each.name, snapshot=None)
        with open(each.name, "wb") as my_blob:
            blob_data = blob_client_instance.download_blob()
            blob_data.readinto(my_blob)

        # reading the downloaded blob file
        df = pd.read_csv(each.name, sep='\t')

        a = 0
        for index,a in df.iterrows():
            if "Sensor Timestamp" in str(a):
                start_index = index
                break
        
        df = pd.read_csv(each.name,sep='\t', header=None)
        df = df.iloc[:start_index+1,:]
        df.rename(columns={0:'Meta_info'} , inplace=True)

        df[['Meta', 'Detail']] = df.Meta_info.str.split(": ", expand = True)
        df.drop(columns=['Meta_info'], inplace=True)
        df.set_index('Meta', inplace=True)

        df1 = pd.read_csv(each.name,sep=',',skiprows=start_index+1)
        os.remove(each.name) # Removing the file downloaded from source
        df1.to_csv("ingest_" + each.name,index=False)

        meta_info = {'Business': 
                    {'Description': 'Laptop position detection',
                    'Created_at': '',
                    'Quality_rating': '',
                    'Data_owner': '',
                    'Security_class': '',
                    'Data_source': ''},
                'Technical': 
                    {'DEVICE': 
                        {'dev_name': df.loc['Device','Detail'],
                        'dev_fw': df.loc['UI Version','Detail'] + df.loc['Algo ver','Detail'].split(' ')[1],
                        'dev_id': None,
                        'dev_loc': None,
                        'dev_orientation': None},
                    'SENSOR':{
                        'PRESSURE': 
                            {'dev_id': None,
                            'name': None,
                            'odr': None,
                            'power_mode': None,
                            'osr_p': None,
                            'osr_t': None,
                            'filter_coefficient': None,
                            'unit': None,
                            'samplingfreq': None},
                        'ACCEL': 
                            {'dev_id': None,
                            'name': None,
                            'range': None,
                            'bw': None,
                            'odr': df.loc['ODR','Detail'],
                            'performance_mode': None,
                            'unit': 'm/s2',
                            'samplingfreq': df.loc['Sampling Rate','Detail']},
                        'GYRO': 
                            {'dev_id': None,
                            'name': None,
                            'range': None,
                            'bw': None,
                            'odr': df.loc['ODR','Detail'],
                            'performance_mode': None,
                            'unit': 'rad/sec',
                            'samplingfreq': df.loc['Sampling Rate','Detail']},
                        'MAG': 
                            {'dev_id': None,
                            'name': None,
                            'power_mode': None,
                            'preset_mode': None,
                            'unit': None,
                            'samplingfreq': None},
                        'PROXIMITY': 
                            {'dev_id': None,
                            'name': None,
                            'odr': None,
                            'power_mode': None,
                            'unit': None,
                            'samplingfreq': None}}},
                'Dataset': 
                {'comments': df.loc['Comments','Detail'],
                    'shuttelboard_nr': None,
                    'start_time': df1.loc[0,'Sensor Timestamp'],
                    'stop_time': df1.loc[len(df1)-1,'Sensor Timestamp'],
                    'user_id': df.loc['UserID','Detail'],
                    'age': None,
                    'gender': None,
                    'height': None,
                    'weight': None,
                    'country': None,
                    'nationality': None,
                    'experience': None,
                    'dominant_hand':None}}
        
        for each_member in list(user_info.index):
            if '_'+ each_member +'_' in each.name:
                meta_info['Dataset']['gender'] = user_info.loc[each_member,'Gender']
                meta_info['Dataset']['height'] = user_info.loc[each_member,'Height']
                meta_info['Dataset']['weight'] = user_info.loc[each_member,'Weight']
            else:
                continue
            
            if int(each_member[1:]) < 200:
                meta_info['Dataset']['country'] = "IND"
                if 'HP Pavilion' in df.loc['Device','Detail']:
                    meta_info['Technical']['SENSOR']['ACCEL']['name'] = user_info.loc['U101','Pavilion_Keyboard'] + '(B),' + user_info.loc['U101','Pavilion_Screen'] + '(L)'
                    meta_info['Technical']['SENSOR']['GYRO']['name'] = user_info.loc['U101','Pavilion_Screen'] + '(L)'
                elif 'HP Spectre' in df.loc['Device','Detail']:
                    meta_info['Technical']['SENSOR']['ACCEL']['name'] = user_info.loc['U101','Spectre_Keyboard'] + '(B),' + user_info.loc['U101','Spectre_Screen'] + '(L)'
                    meta_info['Technical']['SENSOR']['GYRO']['name'] = user_info.loc['U101','Spectre_Screen'] + '(L)'
            elif 200 < int(each_member[1:]) < 300:
                meta_info['Dataset']['country'] = "KU"
            else:
                meta_info['Dataset']['country'] = "TAIWAN"

        file=open(os.path.join(each.name.split('.')[0]) + ".yaml","w")
        yaml.dump(meta_info,file,sort_keys=False)
        file.close()

        # uploading the yaml file
        container_client2 = blob_service_client.get_container_client(container="01-ingest-meta")
        with open(file=each.name.split('.')[0]+".yaml", mode="rb") as data:
                blob_client = container_client2.upload_blob(name=each.name.split('.')[0] + ".yaml", data=data)
        
        os.remove(each.name.split('.')[0]+".yaml")

        # uploading the csv file without meta information
        container_client3 = blob_service_client.get_container_client(container="01-ingest-logs")
        with open(file="ingest_" + each.name.split('.')[0]+".csv", mode="rb") as data:
                blob_client = container_client3.upload_blob(name= "ingest_"+ each.name.split('.')[0] + ".csv", data=data)

        os.remove("ingest_" + each.name.split('.')[0]+".csv")

    return True