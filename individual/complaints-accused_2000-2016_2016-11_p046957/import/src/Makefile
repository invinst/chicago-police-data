# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-accused_2000-2016_2016-11_p046957/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_accused.csv.gz

all: \
		output/complaints-accused_2000-2016_2016-11.csv.gz

output/complaints-accused_2000-2016_2016-11.csv.gz: \
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
