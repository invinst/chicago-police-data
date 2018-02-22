# -*- coding: utf-8 -*-

# Authors:	Roman Rivera (Invisible Institute)
# individual/complaints-witnesses_2000-2016_2016-11_p046957/import/src/Makefile

.PHONY: all clean
# Additional files created: import.log, import.yaml,
# metadata_complaints-witnesses_2000-2016_2016-11.csv.gz

all: \
		output/complaints-witnesses_2000-2016_2016-11.csv.gz

output/complaints-witnesses_2000-2016_2016-11.csv.gz: \
		src/import.py \
                input/p046957_-_report_3_-_police_officer_witness_data_xi.xls
	python $<


clean:
	rm output/*


# done
