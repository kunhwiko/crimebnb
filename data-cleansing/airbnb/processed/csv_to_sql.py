import csv 

files = ["host", "listing", "amenities", "ratings", "neighborhood"]

for file in files:
    with open(file + '.csv') as csv_file:
        csv_reader = csv.reader(csv_file)
        header = ""
        first_row = True 

        for row in csv_reader:
            if first_row:
                header += "INSERT INTO tmp_airbnb." + file + "(" 
                first_col = True 

                for val in row:
                    if first_col:
                        first_col = False
                        continue 
                    header += val + ", "
                header = header[:-2] + ") "
                first_row = False 
                continue 
            
            values = "VALUES ("
            first_col = True 
            for val in row:
                if first_col == True:
                    first_col = False  
                    continue 
                if val.strip() == "":
                    values += "null, "
                else:
                    word = ""
                    if "'" in val or "`" in val or '"' in val:
                        for ch in val:
                            if ch == "'":
                                word += "'"
                            if ch == "`" or ch == '"':
                                continue 
                            word += ch 
                    else:
                        word = val 
                    
                    values += '"' + word + '"' + ", "
            values = values[:-2] + ") "

            send = open(file + ".txt", "a")
            send.write(header + values + ";\n")
            send.close()

            

            
