# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/complainant/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_complainants.csv.gz

all: \
		output/complainants.csv.gz

output/complainants.csv.gz: \
		src/import.py \
		input/p046957_-_report_5.1_-_complainant_(reporting_party)_data.xls \
                input/p046957_-_report_5.2_-_complainant_(reporting_party)_data.xls \
                input/p046957_-_report_5.3_-_complainant_(reporting_party)_data.xls
	python $<


clean:
	rm output/*


# done
