# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p050032_all-members-rank/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_all-members.csv.gz

all: \
		output/all-members.csv.gz

output/all-members.csv.gz: \
		src/import.py \
		input/P050032_All_Members_Rank.xlsx
	python $<


clean:
	rm output/*


# done
