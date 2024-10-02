# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-complainants_2000-2018_2018-07_18-060-294/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-complainants_2000-2018_2018-07.csv.gz

all: \
		output/complaints-complainants_2000-2018_2018-07.csv.gz

output/complaints-complainants_2000-2018_2018-07.csv.gz: \
		src/import.py \
		input/complainant_export.xlsx
	python3 $<


clean:
	rm output/*


# done
