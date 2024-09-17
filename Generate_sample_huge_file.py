sample_row = "aaa|bbb|111|444"
num_copies = 100
sample_unique_field='111'
base_file="C:\\Users\\45158649\\Downloads\\Harsh.DAT"
modified_file="C:\\Users\\45158649\\Downloads\\Harsh2.DAT"
final_file="C:\\Users\\45158649\\Downloads\\Harsh3.DAT"

################### copy rows
with open(base_file, 'a') as f:
    for _ in range(num_copies):
        f.write(sample_row + '\n')
        
################### Modify the unique column   
with open(base_file, 'r') as file:
    rows = file.readlines()
    
num_rows = len(rows)

with open(modified_file, 'w') as file:
    for i, row in enumerate(rows):
        modified_field = sample_unique_field[:-len(str(num_rows))] + str(i +1)
        
        modified_row = modified_field + row[len(sample_unique_field):]
        
        file.write(modified_row) 

########### Add header and trailer records

with open(modified_file, 'r') as file:
    rows = file.readlines()
    
num_rows = len(rows)
num_rows_str = str(num_rows).zfill(10)

with open(final_file, 'w') as file:
    file.write("HR|20160205|HUB|0|0|\n")
    file.writelines(rows)
    file.write(f"TR|{num_rows_str}|20160205|")
    #file.write(f"Trailer: Totalrows: {num_rows}")
    #TR|{num_rows}|20160205| 
    print ("kuch to bolo")       
