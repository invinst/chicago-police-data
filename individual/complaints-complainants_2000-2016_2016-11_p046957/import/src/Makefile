# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-complainants_2000-2016_2016-11_p046957/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-complainants_2000-2016_2016-11.csv.gz

all: \
		output/complaints-complainants_2000-2016_2016-11.csv.gz

output/complaints-complainants_2000-2016_2016-11.csv.gz: \
		src/import.py \
		input/p046957_-_report_5.1_-_complainant_(reporting_party)_data.xls \
                input/p046957_-_report_5.2_-_complainant_(reporting_party)_data.xls \
                input/p046957_-_report_5.3_-_complainant_(reporting_party)_data.xls
	python $<


clean:
	rm output/*


# done
