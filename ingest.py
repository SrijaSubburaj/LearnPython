import pandas as pd
import yaml
import os

def ingestion_step():
    user_info = pd.read_excel(r'C:\Users\ruv5cob\Desktop\My files\LPD\User_info.xlsx')
    user_info.set_index('User',inplace=True)

    main_path = r'C:\Users\ruv5cob\Desktop\Test\Laptop_position_detection\v1\data'
    list_of_files = os.listdir(os.path.join(main_path,"01_Source"))
    for each in list_of_files:
        print(each)
        
        filepath = os.path.join(main_path,"01_Source",each)
        df = pd.read_csv(filepath,sep='\t')

        a = 0
        for index,a in df.iterrows():
            if "Sensor Timestamp" in str(a):
                start_index = index
                break

        df = pd.read_csv(filepath,sep='\t',header=None)
        df = df.iloc[:start_index+1,:]
        df.rename(columns={0:'Meta_info'},inplace=True)

        df[['Meta', 'Detail']] = df.Meta_info.str.split(": ", expand = True)
        df.drop(columns=['Meta_info'],inplace=True)
        df.set_index('Meta',inplace=True)

        df1 = pd.read_csv(filepath,sep=',',skiprows=start_index+1)
        df1.to_csv(os.path.join(main_path,"DP01_Data_Ingestion","01_Logs",each),index=False)
        
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
                        'dev_fw': df.loc['Algo ver','Detail'],
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
            if '_'+each_member+'_' in each:
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
                
        file=open(os.path.join(main_path,"DP01_Data_Ingestion","02_Meta",each[:-4]) + ".yaml","w")
        yaml.dump(meta_info,file,sort_keys=False)
        file.close()
    
    return True