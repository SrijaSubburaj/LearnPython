import zipfile

extract='./landing/customers-100.zip'
destination="./raw/"
archive = zipfile.ZipFile(extract)

for file in archive.namelist():
        archive.extract(file, destination)