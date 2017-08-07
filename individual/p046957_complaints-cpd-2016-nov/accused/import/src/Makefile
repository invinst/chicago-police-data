# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/accused/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_accused.csv.gz

all: \
		output/accused.csv.gz

output/accused.csv.gz: \
		src/import.py \
		input/p046957_-_report_2.1_-_identified_accused.xls \
                input/p046957_-_report_2.2_-_identified_accused.xls \
                input/p046957_-_report_2.3_-_identified_accused.xls \
                input/p046957_-_report_2.4_-_identified_accused.xls \
                input/p046957_-_report_2.5_-_identified_accused.xls
	python $<


clean:
	rm output/*


# done
