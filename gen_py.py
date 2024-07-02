import gdown
url = 'https://drive.google.com/uc?id=1-tuUof0dcWIrAygZoNjBTLjtLdRKA38E'
file_name = __file__.split('\\')[-1]
path = __file__.replace(file_name, '')
folder_name = "..\test.zip"
path += folder_name
output = path

gdown.download(url, output)

