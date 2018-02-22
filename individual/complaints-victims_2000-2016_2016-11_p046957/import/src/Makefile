# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-victims_2000-2016_2016-11_p046957/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-victims_2000-2016_2016-11.csv.gz

all: \
		output/complaints-victims_2000-2016_2016-11.csv.gz

output/complaints-victims_2000-2016_2016-11.csv.gz: \
		src/import.py \
                input/p046957_-_report_4_-_victim_data.xls
	python $<


clean:
	rm output/*


# done
