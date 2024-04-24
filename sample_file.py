import json
from tqdm import tqdm 

# Size of the target file in bytes (15 GB)
target_size = 15 * 1024 * 1024 * 1024  # 15 GB in bytes


def create_chunk(input_file, output_file, target_size,filter_key = 'also_buy'):
    current_size = 0
    with open(input_file, 'r' , encoding='utf-8') as infile:
        with open(output_file, 'w' , encoding='utf-8') as outfile:
            for line in tqdm(infile):
                record = json.loads(line)
                
                if record.get(filter_key):
                    outfile.write(json.dumps(record) + '\n')
                    current_size+=len(line.encode('utf-8'))
            
                
                print(f"{current_size/1024**3:.2f}")
                
                if current_size >= target_size:
                    break

create_chunk('All_Amazon_Meta.json', 'assignment_Data.json', target_size)
