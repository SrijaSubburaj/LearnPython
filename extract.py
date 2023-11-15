from zipfile import ZipFile

# Function to extract files from a zip archive
def Extract():
    # Path to the zip file to be extracted
    extract = './landing/customers-100.zip'
    # Destination directory where the files will be extracted
    destination = "./raw/"
    # Open the zip file using ZipFile
    archive = ZipFile(extract)
    # Iterate through each file in the zip archive
    for file in archive.namelist():
        # Extract the file to the specified destination directory
        archive.extract(file, destination)


