import wget


url="https://github.com/datablist/sample-csv-files/raw/main/files/customers/customers-100.zip"
directory="./landing/"
wget.download(url,out=directory)




