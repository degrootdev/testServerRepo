file_name = __file__.split('/')[-1]
path = __file__.replace(file_name, '')
in_folder = path + "test/dataset/Lschijf_met_autorisatiematrix"
label_folder = path + "test/dataset/Lschijf_met_autorisatiematrix_gelabeld"
print(in_folder)