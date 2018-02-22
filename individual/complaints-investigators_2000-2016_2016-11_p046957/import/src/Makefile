# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-investigators_2000-2016_2016-11_p046957/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-investigators_2000-2016_2016-11.csv.gz,

all: \
		output/complaints-investigators_2000-2016_2016-11.csv.gz

output/complaints-investigators_2000-2016_2016-11.csv.gz: \
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
