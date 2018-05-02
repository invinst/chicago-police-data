# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-accused_2000-2018_2018-03_18-060-157/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-accused_2000-2018_2018-03.csv.gz

all: \
		output/complaints-accused_2000-2018_2018-03.csv.gz

output/complaints-accused_2000-2018_2018-03.csv.gz: \
		src/import.py \
		input/18-21_accused_updated_no_sql_sterilizied.csv.gz
	python $<


clean:
	rm output/*


# done
