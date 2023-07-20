#!/usr/bin/env python
# coding: utf-8

# In[23]:


import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from io import BytesIO
container_name = "curated"
blob_name = "curated.parquet"
token_credential = DefaultAzureCredential()

blob_service_client = BlobServiceClient(
    account_url="https://salearn220623.blob.core.windows.net/",token_credential=token_credential)

blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content=blob_client.download_blob().content_as_bytes()
df = pd.read_parquet(BytesIO(blob_content))
print(df)


# In[32]:


gender_grp=df.groupby(['Gender'])
print(type(gender_grp))


# In[33]:


avg_age=gender_grp['Age'].mean()
print(type(avg_age))


# In[26]:


df1 = df['Gender'].value_counts()
print(df1)


# In[34]:


df2=df.sort_values(by='Age')
df2


# In[28]:


import matplotlib.pyplot as plt
import pandas as pd
import pandas as pd
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from io import BytesIO
container_name = "curated"
blob_name = "curated.parquet"
token_credential = DefaultAzureCredential()

blob_service_client = BlobServiceClient(
account_url="https://salearn220623.blob.core.windows.net/",token_credential=token_credential)

blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
blob_content=blob_client.download_blob().content_as_bytes()
df = pd.read_parquet(BytesIO(blob_content))
df1 = df['Gender'].value_counts()

plt.bar(['Female', 'Male'],df1)
plt.xlabel('Gender')
plt.ylabel('Count')
plt.title('Counting Gender')
plt.show()


# In[ ]:




