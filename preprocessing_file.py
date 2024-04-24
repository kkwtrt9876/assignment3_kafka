import json

def batch_processing_helper(batch_Data, outputfile):
    with open(outputfile, 'a', encoding='utf-8') as outfile:
        for obj in batch_Data:
            data = list(obj['also_buy'])
            data.append(obj['asin'])
            name = 'bucket'
            diction = {name: data}
            outfile.write(json.dumps(diction) + '\n')

def batch_preprocessing(inputfile, outputfile, batchsize):
    with open(inputfile, 'r', encoding='utf-8') as infile:
        count = 0
        while True:
            batch_Data = []
            for _ in range(batchsize):
                line = infile.readline()
                if not line:
                    break  
                json_object = json.loads(line)
                batch_Data.append(json_object)
            if not batch_Data:
                break 
            batch_processing_helper(batch_Data, outputfile)
            
            break
            

batch_preprocessing('assignment_Data.json', 'processed1.json', 100)
