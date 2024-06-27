import gdown
url = 'https://drive.google.com/uc?id=1-tuUof0dcWIrAygZoNjBTLjtLdRKA38E'
output = './testServerRepo-main/test.zip'
md5 = "md5:fa837a88f0c40c513d975104edf3da17"

gdown.download(url, output)