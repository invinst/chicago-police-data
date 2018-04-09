# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-complaints_2000-2018_2018-03_18-060-157/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-complaints_2000-2018_2018-03.csv.gz

all: \
		output/complaints-complaints_2000-2018_2018-03.csv.gz

output/complaints-complaints_2000-2018_2018-03.csv.gz: \
		src/import.py \
		input/FOIA_Data_157_(Stecklow)_1.2.xlsx
	python $<


clean:
	rm output/*


# done
