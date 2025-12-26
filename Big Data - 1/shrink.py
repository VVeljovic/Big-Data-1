import random

INPUT = "./final_dataset/cleaned_dataset.csv"
OUTPUT = "local_dataset.csv"
SAMPLE_RATE = 0.18  

with open(INPUT, "r", encoding="utf-8", errors="ignore") as fin, \
     open(OUTPUT, "w", encoding="utf-8") as fout:

    header = fin.readline()
    fout.write(header)

    columns = header.strip().split(",")
    start_idx = columns.index("Start_Time")
    end_idx   = columns.index("End_Time")

    for line in fin:
        if random.random() < SAMPLE_RATE:
            parts = line.strip().split(",")

            fout.write(",".join(parts) + "\n")

print("DONE")
