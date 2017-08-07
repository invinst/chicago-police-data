# -*- coding: utf-8 -*-

# Authors:	Roman Rivera
# individual/p046957_complaints-cpd-2016-nov/complaints/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml, metadata_complaints.csv.gz,
# investigators.csv.gz, metadata_investigators.csv.gz

all: \
		output/complaints.csv.gz

output/complaints.csv.gz: \
		src/import.py \
		input/p046957_-_report_1.1_-_all_complaints_in_time_frame.xls \
                input/p046957_-_report_1.2_-_all_complaints_in_time_frame.xls \
                input/p046957_-_report_1.3_-_all_complaints_in_time_frame.xls \
                input/p046957_-_report_1.4_-_all_complaints_in_time_frame.xls \
                input/p046957_-_report_1.5_-_all_complaints_in_time_frame.xls \
                input/p046957_-_report_1.6_-_all_complaints_in_time_frame.xls
	python $<


clean:
	rm output/*


# done
