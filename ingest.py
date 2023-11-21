import wget
def ingest():  
    #Initialize the url
    url="https://github.com/datablist/sample-csv-files/raw/main/files/customers/customers-100.zip"
    #Initialize the designated folder
    directory="./landing/"
    #Using wget to download the file in the designated folder
    wget.download(url,out=directory)



