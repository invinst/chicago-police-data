# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-complainants_1967-1999_2016-12_/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-complainants_1967-1999_2016-12.csv.gz

all: \
		output/complaints-complainants_1967-1999_2016-12.csv.gz

output/complaints-complainants_1967-1999_2016-12.csv.gz: \
		src/import.py \
		input/NEW_WITNESS_FILE_NOV_29_2016_-_no_emp_number.csv
	python $<


clean:
	rm output/*


# done
